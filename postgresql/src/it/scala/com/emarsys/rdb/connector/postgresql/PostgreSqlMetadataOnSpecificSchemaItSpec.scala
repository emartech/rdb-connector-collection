package com.emarsys.rdb.connector.postgresql

import java.util.Properties

import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.postgresql.PostgreSqlConnector.createUrl
import com.emarsys.rdb.connector.postgresql.utils.TestHelper
import com.emarsys.rdb.connector.test.MetadataItSpec
import slick.util.AsyncExecutor

import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import slick.jdbc.PostgresProfile.api._

class PostgreSqlMetadataOnSpecificSchemaItSpec extends MetadataItSpec {

  val schemaName = "otherschema"
  Await.result(TestHelper.executeQuery("CREATE SCHEMA IF NOT EXISTS " + schemaName), 5.seconds)

  val configWithSchema = TestHelper.TEST_CONNECTION_CONFIG.copy(
    connectionParams = "currentSchema=" + schemaName
  )

  private lazy val db: Database = {
    Database.forURL(
      url = createUrl(configWithSchema),
      driver = "org.postgresql.Driver",
      user = configWithSchema.dbUser,
      password = configWithSchema.dbPassword,
      prop = new Properties(),
      executor = AsyncExecutor.default()
    )

  }

  def executeQuery(sql: String): Future[Int] = {
    db.run(sqlu"""#$sql""")
  }

  val connector: Connector =
    Await.result(PostgreSqlConnector(configWithSchema)(AsyncExecutor.default()), 5.seconds).right.get

  override val awaitTimeout = 15.seconds

  def initDb(): Unit = {
    val createTableSql = s"""CREATE TABLE "$tableName" (
                            |    PersonID int,
                            |    LastName varchar(255),
                            |    FirstName varchar(255),
                            |    Address varchar(255),
                            |    City varchar(255)
                            |);""".stripMargin

    val createViewSql = s"""CREATE VIEW "$viewName" AS
                           |SELECT PersonID, LastName, FirstName
                           |FROM "$tableName";""".stripMargin
    Await.result(for {
      _ <- executeQuery(createTableSql)
      _ <- executeQuery(createViewSql)
    } yield (), 15.seconds)
  }

  def cleanUpDb(): Unit = {
    val dropViewSql  = s"""DROP VIEW "$viewName";"""
    val dropTableSql = s"""DROP TABLE "$tableName";"""
    Await.result(for {
      _ <- executeQuery(dropViewSql)
      _ <- executeQuery(dropTableSql)
    } yield (), 15.seconds)
  }

}
