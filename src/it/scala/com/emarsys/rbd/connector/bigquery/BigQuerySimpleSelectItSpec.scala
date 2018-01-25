package com.emarsys.rbd.connector.bigquery

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.testkit.TestKit
import akka.util.Timeout
import com.emarsys.rbd.connector.bigquery.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.test.SimpleSelectItSpec
import org.scalatest._

import scala.concurrent.duration._

class BigQuerySimpleSelectItSpec extends TestKit(ActorSystem()) with SimpleSelectItSpec with WordSpecLike with Matchers with BeforeAndAfterAll with SelectDbInitHelper {

  override implicit val sys: ActorSystem = system
  override implicit val materializer: ActorMaterializer = ActorMaterializer()
  override implicit val timeout: Timeout = 10.seconds
  override val awaitTimeout = timeout.duration

  override def afterAll(): Unit = {
    cleanUpDb()
    connector.close()
    shutdown()
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  "list table values with EQUAL on booleans with case-insensitive true/false values" in {
    val simpleSelect = SimpleSelect(AllField, TableName(aTableName), where = Some(EqualToValue(FieldName("A3"), Value("trUE"))))

    val result = getSimpleSelectResult(simpleSelect)

    checkResultWithoutRowOrder(result, Seq(
      Seq("A1", "A2", "A3"),
      Seq("v1", "1", "1"),
      Seq("v3", "3", "1")
    ))
  }
}
