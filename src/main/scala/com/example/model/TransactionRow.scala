package com.example.model

import java.time.Instant
import java.util.UUID

case class TransactionRow(
  id: UUID,
  limitOrderId: String,
  market: String,
  side: Side,
  amount: BigDecimal,
  status: Status,
  createdAt: Instant
)
