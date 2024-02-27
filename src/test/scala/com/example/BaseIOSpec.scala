package com.example

import cats.effect.{IO, Resource}
import cats.effect.testing.scalatest.{AsyncIOSpec, CatsResourceIO}
import com.example.BaseIOSpec.Fixtures
import com.example.config.DatabaseConfig
import com.example.persistence.{DatabasePool, FlywayMigration}
import org.scalatest.FixtureAsyncTestSuite
import org.scalatest.matchers.should.Matchers
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import skunk.Command
import skunk.data.Completion
import skunk.implicits.toStringOps

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}

object BaseIOSpec {

  final case class Fixtures[F[_]](
    databasePool: DatabasePool[F]
  )

}

trait BaseIOSpec extends AsyncIOSpec with Matchers with CatsResourceIO[Fixtures[IO]] {
  this: FixtureAsyncTestSuite =>
  implicit override def executionContext: ExecutionContext = ExecutionContext.global
  implicit val logger: Logger[IO]                          = Slf4jLogger.getLogger

  val databaseConfig: DatabaseConfig = DatabaseConfig()

  override val ResourceTimeout: FiniteDuration = 2.minutes

  override val resource: Resource[IO, Fixtures[IO]] = {
    for {
      databasePool <- DatabasePool[IO](databaseConfig)
      _            <- Resource.eval(FlywayMigration.migrate[IO](databaseConfig))
    } yield Fixtures(databasePool)
  }

  def truncateAllTables(databasePool: DatabasePool[IO]): IO[Unit] =
    databasePool.sessionResource.use(_.execute(truncateTables)).void

  private val truncateTables: Command[skunk.Void] = {
    sql"""
         TRUNCATE TABLE
           limit_order
         CASCADE
       """.command
  }
}
