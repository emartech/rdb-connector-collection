package com.emarsys.rdb.connector.hana

import com.emarsys.rdb.connector.common.defaults.SqlWriter._
import com.emarsys.rdb.connector.common.defaults.{DefaultSqlWriters, SqlWriter}
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._

object HanaWriters extends DefaultSqlWriters {
  implicit override lazy val tableNameWriter: SqlWriter[TableName] = createTableNameWriter("\"", "\\")
  implicit override lazy val fieldNameWriter: SqlWriter[FieldName] = createFieldNameWriter("\"", "\\")
  implicit override lazy val valueWriter: SqlWriter[Value]         = createValueWriter("'", "'")

  def selectWithGroupLimitWriter(groupLimit: Int, references: Seq[String]): SqlWriter[SimpleSelect] =
    (ss: SimpleSelect) => {
      import com.emarsys.rdb.connector.common.defaults.SqlWriter._

      val partitionFields = references.map(FieldName).map(_.toSql).mkString(",")
      s"""SELECT * FROM (
         |  SELECT *, ROW_NUMBER() OVER (PARTITION BY $partitionFields) AS ROW_NUMBER FROM (
         |    ${ss.toSql}
         |  ) tmp1
         |) tmp2 WHERE ROW_NUMBER <= $groupLimit;""".stripMargin
    }

}
