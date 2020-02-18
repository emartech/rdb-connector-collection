package com.emarsys.rdb.connector.mssql

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.models.DataManipulation.{Criteria, FieldValueWrapper, Record, UpdateDefinition}
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper.NullValue
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import com.emarsys.rdb.connector.mssql.MsSqlWriters._
import slick.jdbc.MySQLProfile.api._

import scala.concurrent.Future

trait MsSqlRawDataManipulation {
  self: MsSqlConnector =>

  import cats.instances.future._
  import cats.instances.list._
  import cats.syntax.traverse._

  override def rawUpdate(tableName: String, definitions: Seq[UpdateDefinition]): ConnectorResponse[Int] = {
    if (definitions.isEmpty) {
      Future.successful(Right(0))
    } else {
      val table = TableName(tableName).toSql
      val queries = definitions.map { definition =>
        val setPart = createSetQueryPart(definition.update)

        val wherePart = createConditionQueryPart(definition.search).toSql

        sqlu"UPDATE #$table SET #$setPart WHERE #$wherePart"
      }

      db.run(DBIO.sequence(queries).transactionally)
        .map(results => Right(results.sum))
        .recover(eitherErrorHandler())
    }
  }

  override def rawInsertData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = {
    if (definitions.isEmpty) {
      Future.successful(Right(0))
    } else {
      db.run(sqlu"#${createInsertQuery(tableName, definitions)}")
        .map(result => Right(result))
        .recoverWith(onDuplicateKey(insertOneByOne(tableName, definitions)))
        .recover(eitherErrorHandler())
    }
  }

  private def insertOneByOne(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] =
    definitions
      .map(List(_))
      .toList
      .traverse { record =>
        db.run(sqlu"#${createInsertQuery(tableName, record)}")
          .recover(onDuplicateKey(0))
      }
      .map(affectedRows => Right(affectedRows.sum))

  override def rawDelete(tableName: String, criteria: Seq[Criteria]): ConnectorResponse[Int] = {
    if (criteria.isEmpty) {
      Future.successful(Right(0))
    } else {
      val table     = TableName(tableName).toSql
      val condition = Or(criteria.map(createConditionQueryPart)).toSql
      val query     = sqlu"DELETE FROM #$table WHERE #$condition"

      db.run(query)
        .map(result => Right(result))
        .recover(eitherErrorHandler())
    }
  }

  override def rawReplaceData(tableName: String, definitions: Seq[Record]): ConnectorResponse[Int] = {
    val newTableName     = generateTempTableName(tableName)
    val newTable         = TableName(newTableName).toSql
    val table            = TableName(tableName).toSql
    val createTableQuery = sqlu"SELECT * INTO #$newTable FROM #$table WHERE 1=0"
    val dropTableQuery   = sqlu"IF OBJECT_ID(#${Value(newTableName).toSql}, 'U') IS NOT NULL DROP TABLE #$newTable;"

    db.run(createTableQuery)
      .flatMap(
        _ =>
          rawInsertData(newTableName, definitions).flatMap(
            insertedCount =>
              swapTableNames(tableName, newTableName).flatMap(_ => db.run(dropTableQuery).map(_ => insertedCount))
          )
      )
      .recover(eitherErrorHandler())
  }

  private def createInsertQuery(tableName: String, definitions: Seq[Record]) = {
    val table = TableName(tableName).toSql

    val fields    = definitions.head.keySet.toSeq
    val fieldList = fields.map(FieldName(_).toSql).mkString("(", ",", ")")
    val valueList = makeSqlValueList(orderValues(definitions, fields))

    s"INSERT INTO $table $fieldList VALUES $valueList"
  }

  private def swapTableNames(tableName: String, newTableName: String) = {
    val temporaryTableName = generateTempTableName()
    val tablePairs         = Seq((tableName, temporaryTableName), (newTableName, tableName), (temporaryTableName, newTableName))
    val query = DBIO.sequence(tablePairs.map {
      case (tableA, tableB) => sqlu"sp_rename #${Value(tableA).toSql}, #${Value(tableB).toSql}";
    })

    db.run(query)
  }

  private def generateTempTableName(original: String = ""): String = {
    val shortedName = if (original.length > 30) original.take(30) else original
    val id          = java.util.UUID.randomUUID().toString.replace("-", "").take(30)
    shortedName + "_" + id
  }

  private def orderValues(data: Seq[Record], orderReference: Seq[String]): Seq[Seq[FieldValueWrapper]] = {
    data.map(row => orderReference.map(d => row.getOrElse(d, NullValue)))
  }

  private def makeSqlValueList(data: Seq[Seq[FieldValueWrapper]]) = {
    import com.emarsys.rdb.connector.common.defaults.FieldValueConverter._
    import fieldValueConverters._

    data
      .map(_.map(_.toSimpleSelectValue.map(_.toSql).getOrElse("NULL")).mkString(", "))
      .mkString("(", "),(", ")")
  }

  private def createConditionQueryPart(criteria: Criteria) = {
    import com.emarsys.rdb.connector.common.defaults.FieldValueConverter._
    import fieldValueConverters._

    And(
      criteria
        .mapValues(_.toSimpleSelectValue)
        .map {
          case (field, Some(value)) => EqualToValue(FieldName(field), value)
          case (field, None)        => IsNull(FieldName(field))
        }
        .toList
    )
  }

  private def createSetQueryPart(criteria: Map[String, FieldValueWrapper]) = {
    import com.emarsys.rdb.connector.common.defaults.FieldValueConverter._
    import fieldValueConverters._

    criteria
      .mapValues(_.toSimpleSelectValue)
      .map {
        case (field, Some(value)) => EqualToValue(FieldName(field), value).toSql
        case (field, None)        => FieldName(field).toSql + "=NULL"
      }
      .mkString(", ")
  }

}
