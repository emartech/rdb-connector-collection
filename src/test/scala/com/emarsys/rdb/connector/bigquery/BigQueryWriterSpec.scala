package com.emarsys.rdb.connector.bigquery

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.bigquery.BigQueryConnector.BigQueryConnectionConfig
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import com.emarsys.rdb.connector.common.defaults.SqlWriter._

class BigQueryWriterSpec extends TestKit(ActorSystem()) with WordSpecLike with Matchers with BeforeAndAfterAll {

  private val dataset = "dataset123"
  val bigQueryConnector = new BigQueryConnector(system, BigQueryConnectionConfig("", dataset, "", ""))(null)

  override protected def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "BigQuerySqlWriters" when {

    "SimpleSelect" should {

      "use bigquery writer - full" in {

        import bigQueryConnector._

        val select = SimpleSelect(
          fields = SpecificFields(Seq(FieldName("""FIELD1"""), FieldName("FIELD2"), FieldName("FIELD3"))),
          table = TableName("TABLE1"),
          where = Some(And(Seq(IsNull(FieldName("FIELD1")), And(Seq(IsNull(FieldName("FIELD2")), EqualToValue(FieldName("FIELD3"), Value("VALUE3"))))))),
          limit = Some(100)
        )

        createWritableSqlElement(select).toSql shouldEqual s"""SELECT FIELD1,FIELD2,FIELD3 FROM $dataset.TABLE1 WHERE (FIELD1 IS NULL AND (FIELD2 IS NULL AND FIELD3="VALUE3")) LIMIT 100"""
      }
    }
  }
}
