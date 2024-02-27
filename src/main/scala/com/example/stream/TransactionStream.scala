package com.example.stream

import cats.data.EitherT
import cats.effect.{Ref, Resource}
import cats.effect.kernel.Async
import cats.effect.std.Queue
import fs2.Stream
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import com.example.model.{OrderRow, TransactionRow, Update}
import com.example.persistence.PreparedQueries
import skunk._

import scala.concurrent.duration.FiniteDuration

final class TransactionStream[F[_]](
  operationTimer: FiniteDuration,
  orders: Queue[F, Update],
  session: Resource[F, Session[F]],
  counter: Ref[F, Int], // transaction counter
  stateManager: StateManager[F]
)(implicit F: Async[F], logger: Logger[F]) {

  // helper methods for testing
  def publish(update: Update): F[Unit]                                            = orders.offer(update)
  def getCounter: F[Int]                                                          = counter.get
  def setSwitch(value: Boolean): F[Unit]                                          = stateManager.setSwitch(value)
  def addNewOrder(order: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = stateManager.add(order, insert)
  // helper methods for testing

  def stream: Stream[F, Unit] = {
    Stream
      .fromQueueUnterminated(orders)
      .dropWhile(_.sequence < 0)
      .evalMap(processUpdate)
  }

  // Application should shut down on error,
  // If performLongRunningOperation fails, we don't want to insert/update the records
  // Transactions always have positive amount
  // Order is executed if total == filled
  private def processUpdate(update: Update): F[Unit] = {
    PreparedQueries(session)
      .use { queries =>
        for {
          // db operation 1
          order       <- stateManager.getOrderState(update, queries)
          transaction <- F.pure(update.toTransaction(order))
          orderFromUpdate = update.toOrder(order)
          // parameters for order update
          params = orderFromUpdate.total *: order.status *: order.orderId *: EmptyTuple
          // db operation 2
          _ <- queries.updateOrder.execute(params)
          // db operation 3
          _ <- queries.insertTransaction.execute(transaction)
          _ <- performLongRunningOperation(transaction).value.void.handleErrorWith(th =>
                 logger.error(th)(s"Got error when performing long running IO!")
               )
        } yield ()
      }
  }

  // represents some long running IO that can fail
  private def performLongRunningOperation(transaction: TransactionRow): EitherT[F, Throwable, Unit] = {
    EitherT.liftF[F, Throwable, Unit](
      F.sleep(operationTimer) *>
        stateManager.getSwitch.flatMap {
          case false =>
            counter
              .getAndUpdate(_ + 1)
              .flatMap(count =>
                logger.info(
                  s"Updated counter to $count by transaction with id ${transaction.id} for ${transaction.market}!"
                )
              )
          case true => F.raiseError(throw new Exception("Long running IO failed!"))
        }
    )
  }
}

object TransactionStream {

  def apply[F[_]: Async: Logger](
    operationTimer: FiniteDuration,
    session: Resource[F, Session[F]]
  ): Resource[F, TransactionStream[F]] = {
    Resource.eval {
      for {
        counter      <- Ref.of(0)
        queue        <- Queue.unbounded[F, Update]
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
