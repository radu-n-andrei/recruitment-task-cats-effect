package com.example.stream

import cats.data.EitherT
import cats.effect.kernel.Async
import cats.effect.std.Queue
import cats.effect.{Ref, Resource}
import cats.syntax.all._
import com.example.model.{OrderRow, TransactionRow}
import com.example.persistence.PreparedQueries
import com.example.stream.StateManager.OrderNotFoundException
import fs2.Stream
import org.typelevel.log4cats.Logger
import skunk._

import java.time.Instant
import scala.concurrent.duration.FiniteDuration

// All SQL queries inside the Queries object are correct and should not be changed
final class TransactionStream[F[_]](
  operationTimer: FiniteDuration,
  orders: Queue[F, OrderRow],
  session: Resource[F, Session[F]],
  transactionCounter: Ref[F, Int], // updated if long IO succeeds
  stateManager: StateManager[F]    // utility for state management
)(implicit F: Async[F], logger: Logger[F]) {

  def stream: Stream[F, Unit] = {
    Stream
      .fromQueueUnterminated(orders)
      .evalMap(processUpdate)
  }

  private def streamRemaining: F[Unit] =
    orders.size.flatMap { s =>
      Stream.fromQueueUnterminated(orders).evalMap(processUpdate).take(s).compile.drain
    }

  // Application should shut down on error,
  // If performLongRunningOperation fails, we don't want to insert/update the records
  // Transactions always have positive amount
  // Order is executed if total == filled
  private def processUpdate(updatedOrder: OrderRow): F[Unit] = {
    F.uncancelable(_ =>
      PreparedQueries(session)
        .use { queries =>
          (for {
            // Get current known order state
            state <- stateManager.getOrderState(updatedOrder, queries)
            transaction = TransactionRow(state = state, updated = updatedOrder)
            // parameters for order update
            params = updatedOrder.filled *: state.orderId *: EmptyTuple
            _ <- if (updatedOrder.filled > 0) // TODO move conditions higher up 1st line in an EitherCond
                   performLongRunningOperation(transaction)
                 else EitherT.rightT[F, Throwable](())
          } yield {
            if (updatedOrder.filled != state.filled) // TODO check on this...kinda
              queries.insertTransaction.execute(transaction).void >>
                queries.updateOrder.execute(params).void
            else F.unit
          }).foldF(
            {
              case _: OrderNotFoundException if Instant.now().isBefore(updatedOrder.updatedAt.plusSeconds(5)) =>
                orders.offer(updatedOrder) // TODO add a 1s timer and launch in background
              case err => logger.error(err)("An error occurred during order processing")
            },
            identity
          )
        }
    )
  }

  // FIXME is the transaction actor really tied to the success of the "long running operation" ???
  // represents some long running IO that can fail
  private def performLongRunningOperation(transaction: TransactionRow): EitherT[F, Throwable, Unit] = {
    EitherT(for {
      _ <- F.sleep(operationTimer).attempt.widen[Either[Throwable, Unit]]
      t <-
        stateManager.getSwitch.flatMap {
          case false =>
            transactionCounter
              .updateAndGet(_ + 1)
              .flatMap(count =>
                logger.info(
                  s"Updated counter to $count by transaction with amount ${transaction.amount} for order ${transaction.orderId}!"
                )
              ) map (_.asRight[Throwable])
          case true => F.pure[Either[Throwable, Unit]](new Exception("Long running IO failed!").asLeft)
        }
    } yield t)
  }

  // helper methods for testing
  def publish(update: OrderRow): F[Unit]                                          = orders.offer(update)
  def getCounter: F[Int]                                                          = transactionCounter.get
  def setSwitch(value: Boolean): F[Unit]                                          = stateManager.setSwitch(value)
  def addNewOrder(order: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = stateManager.add(order, insert)
  // helper methods for testing
}

object TransactionStream {

  def apply[F[_]: Async: Logger](
    operationTimer: FiniteDuration,
    session: Resource[F, Session[F]]
  ): Resource[F, TransactionStream[F]] = {
    Resource.make {
      for {
        counter      <- Ref.of(0)
        queue        <- Queue.unbounded[F, OrderRow]
        stateManager <- StateManager.apply
      } yield new TransactionStream[F](
        operationTimer,
        queue,
        session,
        counter,
        stateManager
      )
    }(_.streamRemaining)
  }

  def apply2[F[_]: Async: Logger](
    operationTimer: FiniteDuration,
    session: Resource[F, Session[F]]
  ): Resource[F, TransactionStream[F]] = {
    Resource.eval {
      for {
        counter      <- Ref.of(0)
        queue        <- Queue.unbounded[F, OrderRow]
        stateManager <- StateManager.apply
      } yield new TransactionStream[F](
        operationTimer,
        queue,
        session,
        counter,
        stateManager
      )
    }
  }
}
