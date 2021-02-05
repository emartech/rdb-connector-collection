package com.emarsys.rdb.connector.mssql

import com.emarsys.rdb.connector.common.defaults.{DefaultSqlWriters, SqlWriter}
import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect.{Fields, SpecificFields, Value}

trait MsSqlWriters extends DefaultSqlWriters {
  implicit override lazy val valueWriter: SqlWriter[Value] = (value: Value) => msSqlValueQuoter(Option(value.v))

  implicit override lazy val simpleSelectWriter: SqlWriter[SimpleSelect] = (ss: SimpleSelect) => {
    val distinct      = if (ss.distinct.getOrElse(false)) "DISTINCT " else ""
    val limit         = ss.limit.map("TOP " + _ + " ").getOrElse("")
    val orderedFields = ss.orderBy.map(_.field)
    val fields: Fields = ss.fields match {
      case SimpleSelect.AllField  => SimpleSelect.AllField
      case SpecificFields(fields) => SpecificFields(fields ++ orderedFields)
    }
    val head = s"SELECT $distinct$limit${fields.toSql} FROM ${ss.table.toSql}"

    val where   = ss.where.map(_.toSql).map(" WHERE " + _).getOrElse("")
    val orderBy = ss.orderBy.toSql

    s"$head$where$orderBy"
  }

  protected def msSqlValueQuoter(text: Option[String]): String = {
    text
      .map(_.replace("'", "''"))
      .map(text => s"'$text'")
      .getOrElse("NULL")
  }

}

object MsSqlWriters extends MsSqlWriters
