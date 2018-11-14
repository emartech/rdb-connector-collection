package com.emarsys.rdb.connector.redshift

import com.emarsys.rdb.connector.common.defaults.{DefaultSqlWriters, SqlWriter}
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect.FieldName

object RedshiftSqlWriters extends DefaultSqlWriters {
  def selectWithGroupLimitWriter(groupLimit: Int, references: Seq[String]): SqlWriter[SimpleSelect] =
    (ss: SimpleSelect) => {
      import com.emarsys.rdb.connector.common.defaults.SqlWriter._

      val partitionFields = references.map(FieldName).map(_.toSql).mkString(",")
      s"""select * from (
       |  select *, row_number() over (partition by $partitionFields) from (
       |    ${ss.toSql}
       |  ) tmp1
       |) tmp2 where row_number <= $groupLimit;""".stripMargin
    }
}
