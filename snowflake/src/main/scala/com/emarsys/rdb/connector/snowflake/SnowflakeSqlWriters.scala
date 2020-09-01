package com.emarsys.rdb.connector.snowflake

import com.emarsys.rdb.connector.common.defaults.{DefaultSqlWriters, SqlWriter}
import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._

object SnowflakeSqlWriters extends DefaultSqlWriters {
  def selectWithGroupLimitWriter(groupLimit: Int, references: Seq[String]): SqlWriter[SimpleSelect] =
    (ss: SimpleSelect) => {
      import com.emarsys.rdb.connector.common.defaults.SqlWriter._

      val partitionFields = references.map(FieldName).map(_.toSql).mkString(",")
      s"""select * from (
       |  select *, ROW_NUMBER() over (partition by $partitionFields order by 1) as ROW_NUMBER from (
       |    ${ss.toSql}
       |  ) tmp1
       |) tmp2 where ROW_NUMBER <= $groupLimit;""".stripMargin
    }
}
