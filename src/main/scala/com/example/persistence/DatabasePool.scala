package com.example.persistence

import cats.effect.std.Console
import cats.effect.{Resource, Temporal}
import cats.syntax.option._
import com.example.config.DatabaseConfig
import fs2.io.net.Network
import natchez.Trace.Implicits.noop
import skunk.Session

final case class DatabasePool[F[_]](sessionResource: Resource[F, Session[F]], size: Int)

object DatabasePool {

  def apply[F[_]: Temporal: Network: Console](config: DatabaseConfig): Resource[F, DatabasePool[F]] =
    Session
      .pooled(
        host = config.host,
        port = config.port,
        user = config.user,
        database = config.database,
        password = config.password.some,
        max = config.maxConnections,
        parameters = Map("search_path" -> config.schema) ++ Session.DefaultConnectionParameters,
        readTimeout = config.readTimeout,
        commandCache = config.cacheSize,
        queryCache = config.cacheSize,
        parseCache = config.cacheSize
      )
      .map(DatabasePool[F](_, config.maxConnections))
}
