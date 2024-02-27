package com.example

import cats.effect.{IO, Resource}
import cats.implicits.toTraverseOps
import com.example.model.Side._
import com.example.model.{OrderRow, Status, TransactionRow, Update}
import com.example.persistence.Queries
import com.example.stream.TransactionStream
import org.scalatest.wordspec.FixtureAsyncWordSpec
import skunk._

import java.time.Instant
import scala.concurrent.duration.{DurationInt, FiniteDuration}

class TransactionsStreamSpec extends FixtureAsyncWordSpec with BaseIOSpec {

  case class Resources(
    transactionsStream: TransactionStream[IO],
    getAllOrders: PreparedQuery[IO, Void, OrderRow],
    getAllTransactions: PreparedQuery[IO, Void, TransactionRow],
    insertOrder: PreparedCommand[IO, OrderRow],
    insertTransaction: PreparedCommand[IO, TransactionRow]
  )

  case class Result(counter: Int, orders: List[OrderRow], transactions: List[TransactionRow])

  // timer for long IO operation
  def getResources(
    fxt: FixtureParam,
    timer: FiniteDuration,
    withTruncate: Boolean = true
  ): Resource[IO, Resources] = {
    for {
      _                 <- Resource.eval(IO.whenA(withTruncate)(truncateAllTables(fxt.databasePool)))
      selectOrder       <- fxt.databasePool.sessionResource.evalMap(_.prepare(Queries.getAllOrders))
      selectTransaction <- fxt.databasePool.sessionResource.evalMap(_.prepare(Queries.getAllTransactions))
      insertOrder       <- fxt.databasePool.sessionResource.evalMap(_.prepare(Queries.insertOrder))
      insertTransaction <- fxt.databasePool.sessionResource.evalMap(_.prepare(Queries.insertTransaction))
      stream            <- TransactionStream.apply(timer, fxt.databasePool.sessionResource)
    } yield Resources(stream, selectOrder, selectTransaction, insertOrder, insertTransaction)
  }

  "TransactionsStream" when {
    "processing transactions" must {
      "T1: process one transaction correctly" in { fxt =>
        val ts = Instant.now
        val order = OrderRow(
          orderId = "example_id",
          market = "btc_eur",
          side = Buy,
          price = 90000,
          total = 0.8,
          filled = 0,
          status = Status.Pending,
          createdAt = ts,
          updatedAt = ts
        )

        val firstUpdate =
          Update(
            id = order.orderId,
            market = order.market,
            side = order.side,
            total = order.total,
            filled = 0.8,
            sequence = 0,
            ts
          )

        val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
          for {
            _            <- stream.addNewOrder(order, insertO)
            streamFiber  <- stream.stream.compile.drain.start
            _            <- stream.publish(firstUpdate)
            _            <- IO.sleep(5.seconds)
            _            <- streamFiber.cancel
            counterValue <- stream.getCounter
            orders       <- getO.stream(skunk.Void, 50).compile.toList
            transactions <- getT.stream(skunk.Void, 50).compile.toList
          } yield Result(counterValue, orders, transactions)
        }
        test.map { case Result(counter, orders, transactions) =>
          val updated = orders.find(_.orderId == order.orderId).get
          val txn     = transactions.find(_.limitOrderId == order.orderId).get

          counter shouldBe 1
          updated.filled shouldBe 0.8
          updated.status shouldBe Status.Executed
          txn.amount shouldBe 0.8
        }
      }

      "T2: process zero amount update correctly" in { fxt =>
        val ts = Instant.now
        val order = OrderRow(
          orderId = "example_id",
          market = "btc_eur",
          side = Buy,
          price = 90000,
          total = 0.8,
          filled = 0,
          status = Status.Pending,
          createdAt = ts,
          updatedAt = ts
        )

        val firstUpdate =
          Update(
            id = order.orderId,
            market = order.market,
            side = order.side,
            total = order.total,
            filled = 0,
            sequence = 0,
            ts = ts
          )

        val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
          for {
            _            <- stream.addNewOrder(order, insertO)
            streamFiber  <- stream.stream.compile.drain.start
            _            <- stream.publish(firstUpdate)
            _            <- IO.sleep(5.seconds)
            _            <- streamFiber.cancel
            counterValue <- stream.getCounter
            orders       <- getO.stream(skunk.Void, 50).compile.toList
            transactions <- getT.stream(skunk.Void, 50).compile.toList
          } yield Result(counterValue, orders, transactions)
        }
        test.map { case Result(counter, orders, transactions) =>
          val updated = orders.find(_.orderId == order.orderId).get
          val txn     = transactions.find(_.limitOrderId == order.orderId)

          counter shouldBe 0
          updated.filled shouldBe 0
          updated.status shouldBe Status.Pending
          txn shouldBe empty
        }
      }

      "T3: process multiple transactions correctly" in { fxt =>
        val ts = Instant.now
        val order = OrderRow(
          orderId = "example_id",
          market = "btc_eur",
          side = Buy,
          price = 90000,
          total = 0.8,
          filled = 0,
          status = Status.Pending,
          createdAt = ts,
          updatedAt = ts
        )

        val firstUpdate =
          Update(
            id = order.orderId,
            market = order.market,
            side = order.side,
            total = order.total,
            filled = 0.5,
            sequence = 0,
            ts = ts
          )

        val secondUpdate =
          Update(
            id = "example_id",
            market = order.market,
            side = order.side,
            total = order.total,
            filled = 0.8,
            sequence = 2,
            ts = ts
          )

        val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
          for {
            _            <- stream.addNewOrder(order, insertO)
            streamFiber  <- stream.stream.compile.drain.start
            _            <- stream.publish(firstUpdate)
            _            <- stream.publish(secondUpdate)
            _            <- IO.sleep(5.seconds)
            _            <- streamFiber.cancel
            counterValue <- stream.getCounter
            orders       <- getO.stream(skunk.Void, 50).compile.toList
            transactions <- getT.stream(skunk.Void, 50).compile.toList
          } yield Result(counterValue, orders, transactions)
        }
        test.map { case Result(counter, orders, transactions) =>
          val updated = orders.find(_.orderId == order.orderId).get
          val txn     = transactions.filter(_.limitOrderId == order.orderId)

          counter shouldBe 2
          updated.filled shouldBe 0.8
          updated.status shouldBe Status.Executed
          txn.size shouldBe 2
          txn.map(_.amount).sum shouldBe 0.8
        }
      }

      "T4: not update records when long running IO fails" in { fxt =>
        val ts = Instant.now
        val order = OrderRow(
          orderId = "example_id",
          market = "btc_eur",
          side = Buy,
          price = 90000,
          total = 0.8,
          filled = 0,
          status = Status.Pending,
          createdAt = ts,
          updatedAt = ts
        )

        val firstUpdate =
          Update(
            id = order.orderId,
            market = order.market,
            side = order.side,
            total = order.total,
            filled = 0.8,
            sequence = 0,
            ts = ts
          )

        val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
          for {
            _            <- stream.addNewOrder(order, insertO)
            streamFiber  <- stream.stream.compile.drain.start
            _            <- stream.setSwitch(true)
            _            <- stream.publish(firstUpdate)
            _            <- IO.sleep(5.seconds)
            _            <- streamFiber.cancel
            counterValue <- stream.getCounter
            orders       <- getO.stream(skunk.Void, 50).compile.toList
            transactions <- getT.stream(skunk.Void, 50).compile.toList
          } yield Result(counterValue, orders, transactions)
        }
        test.map { case Result(counter, orders, transactions) =>
          val updated = orders.find(_.orderId == order.orderId).get
          val txn     = transactions.find(_.limitOrderId == order.orderId)

          counter shouldBe 0
          updated.filled shouldBe 0
          updated.status shouldBe Status.Pending
          txn shouldBe empty
        }
      }

      "T5: ignore transaction not present in the state" in { fxt =>
        val ts = Instant.now
        val order = OrderRow(
          orderId = "example_id",
          market = "btc_eur",
          side = Buy,
          price = 90000,
          total = 0.8,
          filled = 0,
          status = Status.Pending,
          createdAt = ts,
          updatedAt = ts
        )

        val firstUpdate =
          Update(
            id = order.orderId,
            market = order.market,
            side = order.side,
            total = order.total,
            filled = 0.8,
            sequence = 0,
            ts = ts
          )

        val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, _, _) =>
          for {
            streamFiber  <- stream.stream.compile.drain.start
            _            <- stream.publish(firstUpdate)
            _            <- IO.sleep(7.seconds)
            _            <- streamFiber.cancel
            counterValue <- stream.getCounter
            orders       <- getO.stream(skunk.Void, 50).compile.toList
            transactions <- getT.stream(skunk.Void, 50).compile.toList
          } yield Result(counterValue, orders, transactions)
        }
        test.map { case Result(counter, orders, transactions) =>
          counter shouldBe 0
          orders shouldBe empty
          transactions shouldBe empty
        }
      }

      "T6: process current update on stream shutdown" in { fxt =>
        val ts = Instant.now
        val order = OrderRow(
          orderId = "example_id",
          market = "btc_eur",
          side = Buy,
          price = 90000,
          total = 0.8,
          filled = 0,
          status = Status.Pending,
          createdAt = ts,
          updatedAt = ts
        )

        val firstUpdate =
          Update(
            id = "example_id",
            market = order.market,
            side = order.side,
            total = order.total,
            filled = 0.8,
            sequence = 0,
            ts = ts
          )

        val test = getResources(fxt, 5.seconds).use { case Resources(stream, getO, getT, insertO, _) =>
          for {
            _            <- stream.addNewOrder(order, insertO)
            streamFiber  <- stream.stream.compile.drain.start
            _            <- stream.publish(firstUpdate)
            _            <- IO.sleep(1.second)
            _            <- streamFiber.cancel
            counterValue <- stream.getCounter
            orders       <- getO.stream(skunk.Void, 50).compile.toList
            transactions <- getT.stream(skunk.Void, 50).compile.toList
          } yield Result(counterValue, orders, transactions)
        }
        test.map { case Result(counter, orders, transactions) =>
          val updated = orders.find(_.orderId == order.orderId).get
          val txn     = transactions.find(_.limitOrderId == order.orderId).get

          counter shouldBe 1
          updated.filled shouldBe 0.8
          txn.amount shouldBe 0.8
        }
      }

      "T7: process all remaining transactions inside the queue" in { fxt =>
        val ts = Instant.now
        val order = OrderRow(
          orderId = "example_id",
          market = "btc_eur",
          side = Buy,
          price = 90000,
          total = 100,
          filled = 0,
          status = Status.Pending,
          createdAt = ts,
          updatedAt = ts
        )

        val update =
          Update(
            id = "example_id",
            market = order.market,
            side = order.side,
            total = order.total,
            filled = 100,
            sequence = 0,
            ts = ts
          )

        // split the update into 100 transactions of amount 1 and add sequence numbers
        val updates = (0 to 99).toList.map(i => update.copy(filled = 1 + i, sequence = i))

        // push the updates into the stream queue, updates will be processed on shutdown
        val runIOs = getResources(fxt, 100.millis).use { case Resources(stream, _, _, insertOrder, _) =>
          for {
            _ <- stream.addNewOrder(order, insertOrder)
            _ <- updates.traverse(stream.publish)
          } yield ()
        }

        // we want to check database state, so there's no truncate of all tables
        val test = runIOs *> getResources(fxt, 5.seconds, withTruncate = false).use {
          case Resources(_, getO, getT, _, _) =>
            for {
              orders       <- getO.stream(skunk.Void, 50).compile.toList
              transactions <- getT.stream(skunk.Void, 50).compile.toList
              // we don't care about the counter
            } yield Result(0, orders, transactions)
        }

        test
          .map { case Result(_, orders, transactions) =>
            val updated = orders.find(_.orderId == order.orderId).get
            val txn     = transactions.filter(_.limitOrderId == order.orderId)

            updated.filled shouldBe 100
            txn.map(_.amount).sum shouldBe 100
            txn.size shouldBe 100
          }
      }

      "T8: kill the application if sequence number was missed" in { fxt =>
        val ts = Instant.now
        val order = OrderRow(
          orderId = "example_id",
          market = "btc_eur",
          side = Buy,
          price = 90000,
          total = 0.8,
          filled = 0,
          status = Status.Pending,
          createdAt = ts,
          updatedAt = ts
        )

        val firstUpdate =
          Update(
            id = order.orderId,
            market = order.market,
            side = order.side,
            total = order.total,
            filled = 0.5,
            sequence = 0,
            ts = ts
          )

        val secondUpdate =
          Update(
            id = "example_id",
            market = order.market,
            side = order.side,
            total = order.total,
            filled = 0.8,
            sequence = 2,
            ts = ts
          )

        val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
          for {
            _            <- stream.addNewOrder(order, insertO)
            streamFiber  <- stream.stream.compile.drain.start
            _            <- stream.publish(firstUpdate)
            _            <- stream.publish(secondUpdate)
            _            <- IO.sleep(5.seconds)
            _            <- streamFiber.cancel
            counterValue <- stream.getCounter
            orders       <- getO.stream(skunk.Void, 50).compile.toList
            transactions <- getT.stream(skunk.Void, 50).compile.toList
          } yield Result(counterValue, orders, transactions)
        }
        test.map { case Result(counter, orders, transactions) =>
          val updated = orders.find(_.orderId == order.orderId).get
          val txn     = transactions.filter(_.limitOrderId == order.orderId)

          counter shouldBe 1
          updated.filled shouldBe 0.5
          updated.status shouldBe Status.Pending
          txn.size shouldBe 1
          txn.map(_.amount).sum shouldBe 0.5
        }
      }

      // Hint: we can drop the update if it is older than 5 seconds and we don't have it in the state.
      "T9: handle race condition where update arrives before addition to the state" in { fxt =>
        val ts = Instant.now
        val order = OrderRow(
          orderId = "example_id",
          market = "btc_eur",
          side = Buy,
          price = 90000,
          total = 0.8,
          filled = 0,
          status = Status.Pending,
          createdAt = ts,
          updatedAt = ts
        )

        val firstUpdate =
          Update(
            id = order.orderId,
            market = order.market,
            side = order.side,
            total = order.total,
            filled = 0.8,
            sequence = 0,
            ts = ts
          )

        val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
          for {
            streamFiber  <- stream.stream.compile.drain.start
            _            <- stream.publish(firstUpdate)
            _            <- IO.sleep(1.second)
            _            <- stream.addNewOrder(order, insertO)
            _            <- IO.sleep(7.seconds)
            _            <- streamFiber.cancel
            counterValue <- stream.getCounter
            orders       <- getO.stream(skunk.Void, 50).compile.toList
            transactions <- getT.stream(skunk.Void, 50).compile.toList
          } yield Result(counterValue, orders, transactions)
        }
        test.map { case Result(counter, orders, transactions) =>
          val updated = orders.find(_.orderId == order.orderId).get
          val txn     = transactions.find(_.limitOrderId == order.orderId).get

          counter shouldBe 1
          updated.filled shouldBe 0.8
          updated.status shouldBe Status.Executed
          txn.amount shouldBe 0.8
        }
      }
    }
  }
}
