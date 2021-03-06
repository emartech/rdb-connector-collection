package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{
  BooleanValue,
  IntValue,
  NullValue,
  StringValue
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.duration._

/*
For positive test results you need to implement an initDb function which creates two tables with the given names and
columns and must insert the sample data.

Tables:
Z(Z1: string, Z2: int, Z3: ?boolean, Z4: string)

(We will reuse these table definitions with these data.
Please use unique and not null constraint on Z1.
Please use index on Z2
Please use index on Z3

Sample data:
Z:
  ("r1", 1, true, "s1")
  ("r2", 2, false, "s2")
  ("r3", 3, NULL, "s3")
  ("r4", 45, true, "s4")
  ("r5", 45, true, "s5")
 */
trait SearchItSpec extends AnyWordSpecLike with Matchers with BeforeAndAfterAll {
  val uuid = uuidGenerate

  val tableName = s"search_table_$uuid"

  val connector: Connector

  val awaitTimeout = 10.seconds
  val queryTimeout = 10.seconds

  val booleanValue0 = "0"
  val booleanValue1 = "1"
  val stringColumn  = "z1"
  val intColumn     = "z2"
  val booleanColumn = "z3"

  implicit val system: ActorSystem

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
  }

  def initDb(): Unit

  def cleanUpDb(): Unit

  s"SearchItSpec $uuid" when {

    "#search" should {
      "find by string" in {
        val result = getConnectorResult(
          connector.search(tableName, Map(stringColumn -> StringValue("r1")), None, queryTimeout),
          awaitTimeout
        )

        checkResultWithoutRowOrder(
          result,
          Seq(
            Seq("Z1", "Z2", "Z3", "Z4"),
            Seq("r1", "1", booleanValue1, "s1")
          )
        )
      }

      "find by int" in {
        val result =
          getConnectorResult(
            connector.search(tableName, Map(intColumn -> IntValue(2)), None, queryTimeout),
            awaitTimeout
          )

        checkResultWithoutRowOrder(
          result,
          Seq(
            Seq("Z1", "Z2", "Z3", "Z4"),
            Seq("r2", "2", booleanValue0, "s2")
          )
        )
      }

      "find by boolean" in {
        val result = getConnectorResult(
          connector.search(tableName, Map(booleanColumn -> BooleanValue(false)), None, queryTimeout),
          awaitTimeout
        )

        checkResultWithoutRowOrder(
          result,
          Seq(
            Seq("Z1", "Z2", "Z3", "Z4"),
            Seq("r2", "2", booleanValue0, "s2")
          )
        )
      }

      "find by null" in {
        val result =
          getConnectorResult(
            connector.search(tableName, Map(booleanColumn -> NullValue), None, queryTimeout),
            awaitTimeout
          )

        checkResultWithoutRowOrder(
          result,
          Seq(
            Seq("Z1", "Z2", "Z3", "Z4"),
            Seq("r3", "3", null, "s3")
          )
        )
      }

      "find by int multiple line" in {
        val result =
          getConnectorResult(
            connector.search(tableName, Map(intColumn -> IntValue(45)), None, queryTimeout),
            awaitTimeout
          )

        checkResultWithoutRowOrder(
          result,
          Seq(
            Seq("Z1", "Z2", "Z3", "Z4"),
            Seq("r4", "45", booleanValue1, "s4"),
            Seq("r5", "45", booleanValue1, "s5")
          )
        )
      }

    }

  }
}
