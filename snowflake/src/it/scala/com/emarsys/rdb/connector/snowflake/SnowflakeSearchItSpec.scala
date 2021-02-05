package com.emarsys.rdb.connector.snowflake

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.snowflake.utils.BaseDbSpec
import com.emarsys.rdb.connector.snowflake.utils.TestHelper.executeQuery
import com.emarsys.rdb.connector.test.SearchItSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class SnowflakeSearchItSpec extends TestKit(ActorSystem("SnowflakeSearchItSpec")) with SearchItSpec with BaseDbSpec {



  override val awaitTimeout = 15.seconds

  override val booleanValue1 = "TRUE"
  override val booleanValue0 = "FALSE"
  override val stringColumn  = "Z1"
  override val intColumn     = "Z2"
  override val booleanColumn = "Z3"

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

  def initDb(): Unit = {
    val createZTableSql =
      s"""CREATE TABLE "$tableName" (
         |    $stringColumn varchar(255) NOT NULL,
         |    $intColumn int,
         |    $booleanColumn boolean,
         |    Z4 varchar(255),
         |    PRIMARY KEY (Z1)
         |);""".stripMargin

    val insertZDataSql =
      s"""INSERT INTO "$tableName" ($stringColumn,$intColumn,$booleanColumn,Z4) VALUES
         |  ('r1', 1, true, 's1'),
         |  ('r2', 2, false, 's2'),
         |  ('r3', 3, NULL, 's3'),
         |  ('r4', 45, true, 's4'),
         |  ('r5', 45, true, 's5')
         |;""".stripMargin

    Await.result(
      for {
        _ <- executeQuery(createZTableSql)
        _ <- executeQuery(insertZDataSql)
      } yield (),
      10.seconds
    )
  }

  def cleanUpDb(): Unit = {
    Await.result(executeQuery(s"""DROP TABLE "$tableName";"""), 15.seconds)
  }
}
