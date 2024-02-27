package com.example.persistence

import com.example.model.{OrderRow, Status, TransactionRow}
import com.example.model.Side.sideCodec
import com.example.model.Status.statusCodec
import skunk.codec.all._
import skunk._
import skunk.syntax.all._

import java.time.{Instant, ZoneOffset}

object Queries {

  implicit val instantCodec: Codec[Instant] = timestamptz.imap(_.toInstant)(_.atOffset(ZoneOffset.UTC))

  private val marketOrderEncoder: Encoder[TransactionRow] =
    (uuid *: varchar *: varchar *: sideCodec *: numeric *: statusCodec *: instantCodec).values.to[TransactionRow]

  private val limitOrderEncoder: Encoder[OrderRow] =
    (varchar *: varchar *: sideCodec *: numeric *: numeric *: numeric *: statusCodec *: instantCodec *: instantCodec).values
      .to[OrderRow]

  val insertTransaction: Command[TransactionRow] = {
    sql"""
         INSERT INTO transactions (id, limit_order_id, market,
         side, amount, status, created_at)
         VALUES $marketOrderEncoder
       """.command
  }

  val getAllTransactions: Query[Void, TransactionRow] = {
    sql"""
         SELECT * FROM transactions
       """
      .query(uuid ~ text ~ text ~ sideCodec ~ numeric ~ statusCodec ~ instantCodec)
      .map { case id ~ orderId ~ market ~ side ~ amount ~ status ~ createdAt =>
        TransactionRow(id, orderId, market, side, amount, status, createdAt)
      }
  }

  val getOrder: Query[String, OrderRow] = {
    sql"""
         SELECT * FROM limit_order
         WHERE order_id = $varchar
       """
      .query(text ~ text ~ sideCodec ~ numeric ~ numeric ~ numeric ~ statusCodec ~ instantCodec ~ instantCodec)
      .map { case id ~ market ~ side ~ price ~ total ~ filled ~ status ~ created ~ updated =>
        OrderRow(id, market, side, price, total, filled, status, created, updated)
      }
  }

  val getAllOrders: Query[Void, OrderRow] = {
    sql"""
         SELECT * FROM limit_order
       """
      .query(text ~ text ~ sideCodec ~ numeric ~ numeric ~ numeric ~ statusCodec ~ instantCodec ~ instantCodec)
      .map { case id ~ market ~ side ~ price ~ total ~ filled ~ status ~ created ~ updated =>
        OrderRow(id, market, side, price, total, filled, status, created, updated)
      }
  }

  val insertOrder: Command[OrderRow] = {
    sql"""
         INSERT INTO limit_order (order_id, market,
         side, price, total, filled, status, created_at, updated_at)
         VALUES $limitOrderEncoder
       """.command
  }

  val updateOrder: Command[BigDecimal *: Status *: String *: EmptyTuple] = {
    sql"""
         UPDATE limit_order
         SET filled = $numeric, status = $statusCodec
         WHERE order_id = $text
       """.command
  }
}
