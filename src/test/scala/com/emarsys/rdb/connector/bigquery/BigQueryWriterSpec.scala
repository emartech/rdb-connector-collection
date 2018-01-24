package com.emarsys.rdb.connector.bigquery

import com.emarsys.rdb.connector.bigquery.BigQueryConnector.BigQueryConnectionConfig
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.FieldModel
import org.scalatest.{Matchers, WordSpecLike}

class BigQueryWriterSpec extends WordSpecLike with Matchers {

  private val dataset = "dataset123"
  val config = BigQueryConnectionConfig("", dataset, "", "")

  "BigQuerySqlWriters" when {

    "SimpleSelect" should {

      "use bigquery writer - only str equalTo" in {

        val writer = BigQueryWriter(config, Seq(FieldModel("FIELD3", "STRING")))
        import writer._

        val where = EqualToValue(FieldName("FIELD3"), Value("VALUE3"))

        createWritableSqlElement(where).toSql shouldEqual s"""FIELD3="VALUE3""""
      }

      "use bigquery writer - only numeric equalTo" in {
        val writer = BigQueryWriter(config, Seq(FieldModel("FIELD2", "INT64"), FieldModel("FIELD3", "FLOAT64")))
        import writer._

        val where = Or(Seq(EqualToValue(FieldName("FIELD2"), Value("123455")), EqualToValue(FieldName("FIELD3"), Value("123.345"))))

        createWritableSqlElement(where).toSql shouldEqual s"""(FIELD2=123455 OR FIELD3=123.345)"""
      }

      Map(
        "1" -> "TRUE",
        "true" -> "TRUE",
        "TRUE" -> "TRUE",
        "0" -> "FALSE",
        "false" -> "FALSE",
        "FALSE" -> "FALSE"
      ) foreach { case (value, sqlValue) =>
        s"use bigquery writer - only boolean equalTo $value -> $sqlValue" in {
          val writer = BigQueryWriter(config, Seq(FieldModel("FIELD2", "BOOL")))
          import writer._

          val where = EqualToValue(FieldName("FIELD2"), Value(value))

          createWritableSqlElement(where).toSql shouldEqual s"""FIELD2=$sqlValue"""
        }
      }

      "use bigquery writer - full with str equalTo" in {

        val writer = BigQueryWriter(config, Seq(FieldModel("FIELD3", "STRING")))
        import writer._

        val select = SimpleSelect(
          fields = SpecificFields(Seq(FieldName("""FIELD1"""), FieldName("FIELD2"), FieldName("FIELD3"))),
          table = TableName("TABLE1"),
          where = Some(And(Seq(IsNull(FieldName("FIELD1")), Or(Seq(IsNull(FieldName("FIELD2")), EqualToValue(FieldName("FIELD3"), Value("VALUE3"))))))),
          limit = Some(100)
        )

        createWritableSqlElement(select).toSql shouldEqual s"""SELECT FIELD1,FIELD2,FIELD3 FROM $dataset.TABLE1 WHERE (FIELD1 IS NULL AND (FIELD2 IS NULL OR FIELD3="VALUE3")) LIMIT 100"""
      }
    }

  }
}
