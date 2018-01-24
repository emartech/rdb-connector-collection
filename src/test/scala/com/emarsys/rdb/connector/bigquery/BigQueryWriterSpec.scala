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

      Map(
        "INT64" -> "123",
        "FLOAT64" -> "123.123",
        "INTEGER" -> "123",
        "FLOAT" -> "123,123"
      ) foreach { case (numericType, numericValue) =>
        s"use bigquery writer - only numeric equalTo - $numericType" in {
          val writer = BigQueryWriter(config, Seq(FieldModel("FIELD1", numericType)))
          import writer._

          val where = EqualToValue(FieldName("FIELD1"), Value(numericValue))

          createWritableSqlElement(where).toSql shouldEqual s"""FIELD1=$numericValue"""
        }
      }

      Map(
        "BOOL" -> "1" -> "TRUE",
        "BOOL" -> "true" -> "TRUE",
        "BOOL" -> "TRUE" -> "TRUE",
        "BOOL" -> "0" -> "FALSE",
        "BOOL" -> "false" -> "FALSE",
        "BOOL" -> "FALSE" -> "FALSE",
        "BOOLEAN" -> "1" -> "TRUE",
        "BOOLEAN" -> "true" -> "TRUE",
        "BOOLEAN" -> "TRUE" -> "TRUE",
        "BOOLEAN" -> "0" -> "FALSE",
        "BOOLEAN" -> "false" -> "FALSE",
        "BOOLEAN" -> "FALSE" -> "FALSE"
      ) foreach { case ((boolType, boolValue), sqlValue) =>
        s"use bigquery writer - only boolean equalTo $boolValue: $boolType -> $sqlValue" in {
          val writer = BigQueryWriter(config, Seq(FieldModel("FIELD2", boolType)))
          import writer._

          val where = EqualToValue(FieldName("FIELD2"), Value(boolValue))

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
