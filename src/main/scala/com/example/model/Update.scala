package com.example.model

import java.time.Instant
import java.util.UUID

final case class Update(
  id: String,
  market: String,
  side: Side,
  total: BigDecimal,
  filled: BigDecimal,
  sequence: Int,
  ts: Instant
) {

  def toTransaction(limitOrder: OrderRow): TransactionRow = {
    TransactionRow(
      id = UUID.randomUUID(),
      limitOrderId = limitOrder.orderId,
      market = market,
      side = limitOrder.side,
      amount = filled,
      status = Status.Executed,
      createdAt = Instant.now
    )
  }
  def toOrder(order: OrderRow): OrderRow = order.copy(filled = filled)
}
