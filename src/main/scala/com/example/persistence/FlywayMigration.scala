package com.example.persistence

import cats.MonadThrow
import com.example.config.DatabaseConfig
import org.flywaydb.core.Flyway

object FlywayMigration {

  def migrate[F[_]: MonadThrow](config: DatabaseConfig): F[Unit] =
    MonadThrow[F].catchNonFatal(flyway(config).migrate())

  private def flyway(config: DatabaseConfig): Flyway =
    Flyway
      .configure()
      .dataSource(config.url, config.user, config.password)
      .table(config.migrationTable)
      .defaultSchema(config.schema)
      .schemas(config.schema)
      .loggers("slf4j")
      .load()
}
