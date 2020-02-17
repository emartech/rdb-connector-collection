package com.emarsys.rdb.connector.postgresql

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect.{FieldName, SpecificFields, TableName}
import com.emarsys.rdb.connector.postgresql.utils.{SelectDbInitHelper, TestHelper}
import com.emarsys.rdb.connector.test.{getConnectorResult, SimpleSelectItSpec}
import com.emarsys.rdb.connector.test.CustomMatchers.beDatabaseErrorEqualWithoutCause

import scala.concurrent.Await
import scala.concurrent.duration._

class PostgreSqlSimpleSelectItSpec
    extends TestKit(ActorSystem("PostgreSqlSimpleSelectItSpec"))
    with SimpleSelectItSpec
    with SelectDbInitHelper {

  override implicit val materializer: Materializer = ActorMaterializer()

  override val awaitTimeout = 15.seconds

  override def afterAll(): Unit = {
    shutdown()
    super.afterAll()
  }

  val sleepViewName = "sleep_view"

  override def initDb(): Unit = {
    super.initDb()

    val createCTableSql =
      s"""CREATE TABLE "$cTableName" (
         |    C varchar(255) NOT NULL
         |);""".stripMargin

    val createFunction     = """CREATE OR REPLACE FUNCTION do_sleep() RETURNS integer AS $$
                           |        BEGIN
                           |                PERFORM PG_SLEEP(6);
                           |                RETURN 1;
                           |        END;
                           |$$ LANGUAGE plpgsql;""".stripMargin
    val createSleepViewSql = "CREATE VIEW " + sleepViewName + " AS SELECT do_sleep() as sleep".stripMargin

    val insertCDataSql =
      s"""INSERT INTO "$cTableName" (C) VALUES
         |('c12'),
         |('c12'),
         |('c3')
         |;""".stripMargin

    Await.result(
      for {
        _ <- TestHelper.executeQuery(createCTableSql)
        _ <- TestHelper.executeQuery(insertCDataSql)
        _ <- TestHelper.executeQuery(createFunction)
        _ <- TestHelper.executeQuery(createSleepViewSql)
      } yield (),
      5.seconds
    )
  }

  override def cleanUpDb(): Unit = {
    val dropCTableSql = s"""DROP TABLE "$cTableName";"""
    Await.result(for {
      _ <- TestHelper.executeQuery(dropCTableSql)
      _ <- TestHelper.executeQuery("DROP VIEW " + sleepViewName)
    } yield (), 5.seconds)
    super.cleanUpDb()
  }

  "#simpleSelect" should {

    "return QueryTimeout when the query does not terminate within the specified timeout" in {
      val message  = "ERROR: canceling statement due to user request"
      val expected = DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, message, None, None)
      val select   = SimpleSelect(SpecificFields(Seq(FieldName("sleep"))), TableName(sleepViewName))
      val error = the[DatabaseError] thrownBy getConnectorResult(
        connector.simpleSelect(select, 5.seconds),
        awaitTimeout
      )

      error should beDatabaseErrorEqualWithoutCause(expected)
    }

  }

}
