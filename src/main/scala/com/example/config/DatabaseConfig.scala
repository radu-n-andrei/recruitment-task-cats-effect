package com.example.config

import scala.concurrent.duration.Duration

final case class DatabaseConfig(
  host: String = "127.0.0.1",
  port: Int = 5432,
  url: String = "jdbc:postgresql://127.0.0.1:5432/strategy_service",
  user: String = "postgres",
  password: String = "postgres",
  schema: String = "strategy_service",
  database: String = "strategy_service",
  maxConnections: Int = 24,
  migrationTable: String = "flyway_schema_history",
  readTimeout: Duration = Duration.Inf,
  cacheSize: Int = 2048
)
