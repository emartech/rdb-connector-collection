package com.emarsys.rdb.connector.redshift
import java.util.Properties

import com.emarsys.rdb.connector.redshift.RedshiftConnector.createUrl
import com.emarsys.rdb.connector.redshift.utils.{BaseDbSpec, TestHelper}
import com.emarsys.rdb.connector.test.MetadataItSpec
import slick.jdbc.PostgresProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class RedshiftMetadataOnSpecificSchemaItSpec extends MetadataItSpec with BaseDbSpec {
  val schemaName = "otherschema"
  override val connectionConfig =
    TestHelper.TEST_CONNECTION_CONFIG.copy(connectionParams = s"currentSchema=$schemaName")

  Await.result(TestHelper.executeQuery("CREATE SCHEMA IF NOT EXISTS " + schemaName), 5.seconds)

  val configWithSchema = TestHelper.TEST_CONNECTION_CONFIG.copy(
    connectionParams = "currentSchema=" + schemaName
  )

  private lazy val db: Database = {
    Database.forURL(
      url = createUrl(configWithSchema),
      driver = "com.amazon.redshift.jdbc42.Driver",
      user = configWithSchema.dbUser,
      password = configWithSchema.dbPassword,
      prop = new Properties(),
      executor = AsyncExecutor.default()
    )

  }

  def executeQuery(sql: String): Future[Int] = {
    db.run(sqlu"""#$sql""")
  }

  override val awaitTimeout = 15.seconds

  def initDb(): Unit = {
    val createTableSql = s"""CREATE TABLE "$schemaName"."$tableName" (
                            |    PersonID int,
                            |    LastName varchar(255),
                            |    FirstName varchar(255),
                            |    Address varchar(255),
                            |    City varchar(255)
                            |);""".stripMargin

    val createViewSql = s"""CREATE VIEW "$schemaName"."$viewName" AS
                           |SELECT PersonID, LastName, FirstName
                           |FROM "$schemaName"."$tableName";""".stripMargin
    Await.result(for {
      _ <- executeQuery(createTableSql)
      _ <- executeQuery(createViewSql)
    } yield (), 15.seconds)
  }

  def cleanUpDb(): Unit = {
    val dropViewSql  = s"""DROP VIEW IF EXISTS "$schemaName"."$viewName";"""
    val dropTableSql = s"""DROP TABLE IF EXISTS "$schemaName"."$tableName";"""
    Await.result(for {
      _ <- executeQuery(dropViewSql)
      _ <- executeQuery(dropTableSql)
    } yield (), 15.seconds)
  }

}
