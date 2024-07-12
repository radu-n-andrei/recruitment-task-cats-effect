package com.example.stream

import cats.data.EitherT
import cats.effect.kernel.Async
import cats.effect.std.Queue
import cats.effect.{Ref, Resource}
import cats.syntax.all._
import com.example.model.{OrderRow, TransactionRow}
import com.example.persistence.PreparedQueries
import com.example.stream.StateManager.OrderNotFoundException
import com.example.stream.TransactionStream.ValidationException
import fs2.{Pipe, Stream}
import org.typelevel.log4cats.Logger
import skunk._

import java.time.Instant
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.{Duration, FiniteDuration}

// All SQL queries inside the Queries object are correct and should not be changed
final class TransactionStream[F[_]](
  operationTimer: FiniteDuration,
  orders: Queue[F, OrderRow],
  session: Resource[F, Session[F]],
  transactionCounter: Ref[F, Int], // updated if long IO succeeds
  stateManager: StateManager[F],   // utility for state management
  maxConcurrent: Int
)(implicit F: Async[F], logger: Logger[F]) {

  def stream: Stream[F, Unit] = {
    Stream
      .fromQueueUnterminated(orders)
      .parEvalMap(maxConcurrent)(processUpdate)
      .handleErrorWith(_ => Stream.empty)
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
            // trivial input validation
            _ <- orderValidation(
                   updatedOrder.filled > 0
                     && updatedOrder.filled <= updatedOrder.total,
                   "Order update has incorrect arguments"
                 )
            // Get current known order state
            state <- stateManager.getOrderState(updatedOrder, queries)
            // filled order validation
            _ <- orderValidation(state.filled != state.total, "Order is already filled")
            // outdated order validation
            _ <- orderValidation(updatedOrder.filled >= state.filled, "Order is outdated")
            transaction = TransactionRow(state = state, updated = updatedOrder)
            // parameters for order update
            params = updatedOrder.filled *: state.orderId *: EmptyTuple
            _ <- performLongRunningOperation(transaction)
          } yield {
            if (updatedOrder.filled != state.filled) // TODO check on this...
              queries.insertTransaction.execute(transaction).void >>
                queries.updateOrder.execute(params).void
            else F.unit
          }).foldF(
            {
              case _: OrderNotFoundException if Instant.now().isBefore(updatedOrder.updatedAt.plusSeconds(5)) =>
                orders.offer(updatedOrder)
              case _: OrderNotFoundException =>
                logger.warn(s"Order ${updatedOrder.orderId} not received after 5 seconds. Dropping update.")
              case ve: ValidationException => logger.error(ve)("An error occurred during order processing")
              case e: Exception =>
                logger.error(e)("Long running application failed")
              case err: Error =>
                logger.error(err)("Fatal error occurred. Shutting down application") >>
                  F.raiseError[Unit](err)
            },
            identity
          )
        }
    )
  }

  private def orderValidation(p: Boolean, validationMessage: String): EitherT[F, Throwable, Unit] =
    EitherT
      .cond[F](
        p,
        (),
        ValidationException(new Exception(validationMessage))
      )
      .leftWiden[Throwable]

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
    session: Resource[F, Session[F]],
    maxConcurrent: Int
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
        stateManager,
        maxConcurrent
      )
    }(_.streamRemaining)
  }

  final case class ValidationException(e: Exception) extends Throwable

}
