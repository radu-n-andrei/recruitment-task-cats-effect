package com.example.model

import java.time.Instant

case class OrderRow(
  orderId: String,
  market: String,
  side: Side,
  price: BigDecimal,
  total: BigDecimal,
  filled: BigDecimal,
  status: Status,
  createdAt: Instant,
  updatedAt: Instant
)
