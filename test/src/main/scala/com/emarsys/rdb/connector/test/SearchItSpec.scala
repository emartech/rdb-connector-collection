package com.emarsys.rdb.connector.test

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.Connector
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.{BooleanValue, IntValue, NullValue, StringValue}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.Await
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
trait SearchItSpec extends WordSpecLike with Matchers with BeforeAndAfterAll {
  val uuid = uuidGenerate

  val tableName = s"search_table_$uuid"

  val connector: Connector

  val awaitTimeout = 5.seconds
  val queryTimeout = 5.seconds

  implicit val materializer: Materializer

  override def beforeAll(): Unit = {
    initDb()
  }

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
  }

  def initDb(): Unit

  def cleanUpDb(): Unit


  private val headerLineSize = 1

  s"SearchItSpec $uuid" when {

    "#search" should {
      "find by string" in {
        val result = getConnectorResult(connector.search(tableName, Map("z1" -> StringValue("r1")), None, queryTimeout), awaitTimeout)

        checkResultWithoutRowOrder(result, Seq(
          Seq("Z1", "Z2", "Z3", "Z4"),
          Seq("r1", "1", "1", "s1")
        ))
      }

      "find by int" in {
        val result = getConnectorResult(connector.search(tableName, Map("z2" -> IntValue(2)), None, queryTimeout), awaitTimeout)

        checkResultWithoutRowOrder(result, Seq(
          Seq("Z1", "Z2", "Z3", "Z4"),
          Seq("r2", "2", "0", "s2")
        ))
      }

      "find by boolean" in {
        val result = getConnectorResult(connector.search(tableName, Map("z3" -> BooleanValue(false)), None, queryTimeout), awaitTimeout)

        checkResultWithoutRowOrder(result, Seq(
          Seq("Z1", "Z2", "Z3", "Z4"),
          Seq("r2", "2", "0", "s2")
        ))
      }

      "find by null" in {
        val result = getConnectorResult(connector.search(tableName, Map("z3" -> NullValue), None, queryTimeout), awaitTimeout)

        checkResultWithoutRowOrder(result, Seq(
          Seq("Z1", "Z2", "Z3", "Z4"),
          Seq("r3", "3", null, "s3")
        ))
      }

      "find by int multiple line" in {
        val result = getConnectorResult(connector.search(tableName, Map("z2" -> IntValue(45)), None, queryTimeout), awaitTimeout)

        checkResultWithoutRowOrder(result, Seq(
          Seq("Z1", "Z2", "Z3", "Z4"),
          Seq("r4", "45", "1", "s4"),
          Seq("r5", "45", "1", "s5")
        ))
      }

    }

  }
}
