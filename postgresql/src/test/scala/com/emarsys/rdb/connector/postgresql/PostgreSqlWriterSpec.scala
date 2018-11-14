package com.emarsys.rdb.connector.postgresql

import com.emarsys.rdb.connector.common.models.SimpleSelect
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import org.scalatest.{Matchers, WordSpecLike}

class PostgreSqlWriterSpec extends WordSpecLike with Matchers {

  "selectWithGroupLimitWriter" in {
    import PostgreSqlWriters._
    import com.emarsys.rdb.connector.common.defaults.SqlWriter._

    val select = SimpleSelect(
      fields = AllField,
      table = TableName("TABLE1"),
      distinct = Some(true)
    )

    select.toSql(selectWithGroupLimitWriter(777, Seq("a", "b"))) shouldEqual
      """select * from (
        |  select *, row_number() over (partition by "a","b") from (
        |    SELECT DISTINCT * FROM "TABLE1"
        |  ) tmp1
        |) tmp2 where row_number <= 777;""".stripMargin
  }
}
