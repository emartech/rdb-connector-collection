package com.emarsys.rdb.connector.common.defaults

import com.emarsys.rdb.connector.common.models.SimpleSelect._
import org.scalatest.{Matchers, WordSpecLike}
import SqlWriter._
import com.emarsys.rdb.connector.common.models.SimpleSelect

class DefaultSqlWriterSpec extends WordSpecLike with Matchers {

  "DefaultSqlWriter" when {

    "TableName" should {

      "use default writer" in {
        import DefaultSqlWriters._

        TableName("TABLE1").toSql shouldEqual """"TABLE1""""
      }

      "use default writer - escape" in {
        import DefaultSqlWriters._

        TableName("""TA\BLE"1""").toSql shouldEqual """"TA\\BLE\"1""""
      }

      "use custom writer" in {
        val customWriters = new DefaultSqlWriters {
          override implicit lazy val tableNameWriter: SqlWriter[TableName] = SqlWriter.createTableNameWriter("#", "\\")
        }
        import customWriters._

        TableName("TABLE1").toSql shouldEqual "#TABLE1#"
      }
    }

    "FieldName" should {

      "use default writer" in {
        import DefaultSqlWriters._

        FieldName("FIELD1").toSql shouldEqual """"FIELD1""""
      }

      "use default writer - escape" in {
        import DefaultSqlWriters._

        FieldName("""F"IEL\D1""").toSql shouldEqual """"F\"IEL\\D1""""
      }

      "use custom writer" in {
        val customWriters = new DefaultSqlWriters {
          override implicit lazy val fieldNameWriter: SqlWriter[FieldName] = SqlWriter.createFieldNameWriter("#", "\\")
        }
        import customWriters._

        FieldName("FIELD1").toSql shouldEqual "#FIELD1#"
      }
    }

    "Value" should {

      "use default writer" in {
        import DefaultSqlWriters._

        Value("VALUE1").toSql shouldEqual "'VALUE1'"
      }

      "use default writer - escape" in {
        import DefaultSqlWriters._

        Value("""VA'L'UE\1""").toSql shouldEqual """'VA\'L\'UE\\1'"""
      }

      "use custom writer" in {
        val customWriters = new DefaultSqlWriters {
          override implicit lazy val valueWriter: SqlWriter[Value] = SqlWriter.createValueWriter("#", "\\")
        }
        import customWriters._

        Value("VALUE1").toSql shouldEqual "#VALUE1#"
      }
    }

    "AllFields" should {

      "use default writer" in {
        import DefaultSqlWriters._

        AllField.toSql shouldEqual "*"
      }

      "use custom writer" in {
        val customWriters = new DefaultSqlWriters {
          override implicit lazy val allFieldWriter: SqlWriter[SimpleSelect.AllField.type] =
            SqlWriter.createAllFieldWriter("#")
        }
        import customWriters._

        AllField.toSql shouldEqual "#"
      }
    }

    "SpecificFields" should {

      "use default writer - one field" in {
        import DefaultSqlWriters._

        SpecificFields(Seq(FieldName("FIELD1"))).toSql shouldEqual """"FIELD1""""
      }

      "use default writer - many fields" in {
        import DefaultSqlWriters._

        SpecificFields(Seq(FieldName("FIELD1"), FieldName("FIELD2"), FieldName("FIELD3"))).toSql shouldEqual """"FIELD1","FIELD2","FIELD3""""
      }

      "use custom writer - many fields" in {
        val customWriters = new DefaultSqlWriters {
          override implicit lazy val specificFieldsWriter: SqlWriter[SpecificFields] =
            SqlWriter.createSpecificFieldsWriter("#")
        }
        import customWriters._

        SpecificFields(Seq(FieldName("FIELD1"), FieldName("FIELD2"), FieldName("FIELD3"))).toSql shouldEqual """"FIELD1"#"FIELD2"#"FIELD3""""
      }
    }

    "IsNull" should {

      "use default writer" in {
        import DefaultSqlWriters._

        IsNull(FieldName("FIELD1")).toSql shouldEqual """"FIELD1" IS NULL"""
      }

      "use custom writer" in {
        val customWriters = new DefaultSqlWriters {
          override implicit lazy val isNullWriter: SqlWriter[IsNull] = SqlWriter.createIsNullWriter("<<IS %s NULL>>")
        }
        import customWriters._

        IsNull(FieldName("FIELD1")).toSql shouldEqual """<<IS "FIELD1" NULL>>"""
      }
    }

    "IsNotNull" should {

      "use default writer" in {
        import DefaultSqlWriters._

        NotNull(FieldName("FIELD1")).toSql shouldEqual """"FIELD1" IS NOT NULL"""
      }

      "use custom writer" in {
        val customWriters = new DefaultSqlWriters {
          override implicit lazy val notNullWriter: SqlWriter[NotNull] =
            SqlWriter.createNotNullWriter("<<IS NOT %s NULL>>")
        }
        import customWriters._

        NotNull(FieldName("FIELD1")).toSql shouldEqual """<<IS NOT "FIELD1" NULL>>"""
      }
    }

    "EqualToValue" should {

      "use default writer" in {
        import DefaultSqlWriters._

        EqualToValue(FieldName("FIELD1"), Value("VALUE1")).toSql shouldEqual """"FIELD1"='VALUE1'"""
      }

      "use custom writer" in {
        val customWriters = new DefaultSqlWriters {
          override implicit lazy val equalToValueWriter: SqlWriter[EqualToValue] =
            SqlWriter.createEqualToValueWriter(" IS ")
        }
        import customWriters._

        EqualToValue(FieldName("FIELD1"), Value("VALUE1")).toSql shouldEqual """"FIELD1" IS 'VALUE1'"""
      }
    }

    "Or" should {

      "use default writer - one condition" in {
        import DefaultSqlWriters._

        Or(Seq(EqualToValue(FieldName("FIELD1"), Value("VALUE1")))).toSql shouldEqual """"FIELD1"='VALUE1'"""
      }

      "use default writer - many condition" in {
        import DefaultSqlWriters._

        Or(Seq(IsNull(FieldName("FIELD1")), EqualToValue(FieldName("FIELD2"), Value("VALUE2")))).toSql shouldEqual """("FIELD1" IS NULL OR "FIELD2"='VALUE2')"""
      }

      "use default writer - inner or" in {
        import DefaultSqlWriters._

        Or(
          Seq(
            IsNull(FieldName("FIELD1")),
            Or(Seq(IsNull(FieldName("FIELD2")), EqualToValue(FieldName("FIELD3"), Value("VALUE3"))))
          )
        ).toSql shouldEqual """("FIELD1" IS NULL OR ("FIELD2" IS NULL OR "FIELD3"='VALUE3'))"""
      }

      "use custom writer - inner or" in {
        val customWriters = new DefaultSqlWriters {
          override implicit lazy val orWriter: SqlWriter[Or] = (x: Or) => conditionWriter(" || ", x.conditions)
        }
        import customWriters._

        Or(
          Seq(
            IsNull(FieldName("FIELD1")),
            Or(Seq(IsNull(FieldName("FIELD2")), EqualToValue(FieldName("FIELD3"), Value("VALUE3"))))
          )
        ).toSql shouldEqual """("FIELD1" IS NULL || ("FIELD2" IS NULL || "FIELD3"='VALUE3'))"""
      }
    }

    "And" should {

      "use default writer - one condition" in {
        import DefaultSqlWriters._

        And(Seq(EqualToValue(FieldName("FIELD1"), Value("VALUE1")))).toSql shouldEqual """"FIELD1"='VALUE1'"""
      }

      "use default writer - many condition" in {
        import DefaultSqlWriters._

        And(Seq(IsNull(FieldName("FIELD1")), EqualToValue(FieldName("FIELD2"), Value("VALUE2")))).toSql shouldEqual """("FIELD1" IS NULL AND "FIELD2"='VALUE2')"""
      }

      "use default writer - inner or" in {
        import DefaultSqlWriters._

        And(
          Seq(
            IsNull(FieldName("FIELD1")),
            And(Seq(IsNull(FieldName("FIELD2")), EqualToValue(FieldName("FIELD3"), Value("VALUE3"))))
          )
        ).toSql shouldEqual """("FIELD1" IS NULL AND ("FIELD2" IS NULL AND "FIELD3"='VALUE3'))"""
      }

      "use custom writer - inner or" in {
        val customWriters = new DefaultSqlWriters {
          override implicit lazy val andWriter: SqlWriter[And] = (x: And) => conditionWriter(" && ", x.conditions)
        }
        import customWriters._

        And(
          Seq(
            IsNull(FieldName("FIELD1")),
            And(Seq(IsNull(FieldName("FIELD2")), EqualToValue(FieldName("FIELD3"), Value("VALUE3"))))
          )
        ).toSql shouldEqual """("FIELD1" IS NULL && ("FIELD2" IS NULL && "FIELD3"='VALUE3'))"""
      }
    }

    "WhereCondition" should {

      "combinate all element - use default writers" in {
        import DefaultSqlWriters._

        val where =
          And(
            Seq(
              IsNull(FieldName("FIELD1")),
              Or(
                Seq(
                  IsNull(FieldName("FIELD2")),
                  EqualToValue(FieldName("FIELD3"), Value("VALUE3")),
                  NotNull(FieldName("FIELD4"))
                )
              ),
              Or(
                Seq(
                  IsNull(FieldName("FIELD5"))
                )
              )
            )
          )

        where.toSql shouldEqual """("FIELD1" IS NULL AND ("FIELD2" IS NULL OR "FIELD3"='VALUE3' OR "FIELD4" IS NOT NULL) AND "FIELD5" IS NULL)"""
      }

      "combinate all element - use custom writers" in {

        val customWriters = new DefaultSqlWriters {
          override implicit lazy val fieldNameWriter: SqlWriter[FieldName] = SqlWriter.createFieldNameWriter("#", "\\")
          override implicit lazy val valueWriter: SqlWriter[Value]         = SqlWriter.createValueWriter("`", "\\")
          override implicit lazy val notNullWriter: SqlWriter[NotNull] =
            SqlWriter.createNotNullWriter("<<IS NOT %s NULL>>")
          override implicit lazy val equalToValueWriter: SqlWriter[EqualToValue] =
            SqlWriter.createEqualToValueWriter(" == ")
          override implicit lazy val isNullWriter: SqlWriter[IsNull] = SqlWriter.createIsNullWriter("NULL(%s)")
          override implicit lazy val orWriter: SqlWriter[Or]         = (x: Or) => conditionWriter(" || ", x.conditions)
          override implicit lazy val andWriter: SqlWriter[And]       = (x: And) => conditionWriter(" && ", x.conditions)
        }
        import customWriters._

        val where =
          And(
            Seq(
              IsNull(FieldName("FIELD1")),
              Or(
                Seq(
                  IsNull(FieldName("FIELD2")),
                  EqualToValue(FieldName("FIELD3"), Value("VALUE3")),
                  NotNull(FieldName("FIELD4"))
                )
              ),
              Or(
                Seq(
                  IsNull(FieldName("FIELD5"))
                )
              )
            )
          )

        where.toSql shouldEqual """(NULL(#FIELD1#) && (NULL(#FIELD2#) || #FIELD3# == `VALUE3` || <<IS NOT #FIELD4# NULL>>) && NULL(#FIELD5#))"""
      }
    }

    "SortCriteria" should {
      "serialize to valid sql" in {
        import DefaultSqlWriters._

        SortCriteria(FieldName("name"), Direction.Ascending).toSql shouldEqual """"name" ASC"""
        SortCriteria(FieldName("name"), Direction.Descending).toSql shouldEqual """"name" DESC"""
      }
    }

    "SimpleSelect" should {

      "use default writer - minimal" in {
        import DefaultSqlWriters._

        val select = SimpleSelect(
          fields = AllField,
          table = TableName("TABLE1")
        )

        select.toSql shouldEqual """SELECT * FROM "TABLE1""""
      }

      "use default writer - disabled distinct" in {
        import DefaultSqlWriters._

        val select = SimpleSelect(
          fields = AllField,
          table = TableName("TABLE1"),
          distinct = Some(false)
        )

        select.toSql shouldEqual """SELECT * FROM "TABLE1""""
      }

      "use default writer - distinct" in {
        import DefaultSqlWriters._

        val select = SimpleSelect(
          fields = AllField,
          table = TableName("TABLE1"),
          distinct = Some(true)
        )

        select.toSql shouldEqual """SELECT DISTINCT * FROM "TABLE1""""
      }

      "use default writer - specific fields" in {
        import DefaultSqlWriters._

        val select = SimpleSelect(
          fields = SpecificFields(Seq(FieldName("FIELD1"), FieldName("FIELD2"), FieldName("FIELD3"))),
          table = TableName("TABLE1")
        )

        select.toSql shouldEqual """SELECT "FIELD1","FIELD2","FIELD3" FROM "TABLE1""""
      }

      "use default writer - where" in {
        import DefaultSqlWriters._

        val select = SimpleSelect(
          fields = AllField,
          table = TableName("TABLE1"),
          where = Some(
            And(
              Seq(
                IsNull(FieldName("FIELD1")),
                And(Seq(IsNull(FieldName("FIELD2")), EqualToValue(FieldName("FIELD3"), Value("VALUE3"))))
              )
            )
          )
        )

        select.toSql shouldEqual """SELECT * FROM "TABLE1" WHERE ("FIELD1" IS NULL AND ("FIELD2" IS NULL AND "FIELD3"='VALUE3'))"""
      }

      "use default writer - limit" in {
        import DefaultSqlWriters._

        val select = SimpleSelect(
          fields = AllField,
          table = TableName("TABLE1"),
          limit = Some(100)
        )

        select.toSql shouldEqual """SELECT * FROM "TABLE1" LIMIT 100"""
      }

      "use default writer - where and limit" in {
        import DefaultSqlWriters._

        val select = SimpleSelect(
          fields = AllField,
          table = TableName("TABLE1"),
          where = Some(
            And(
              Seq(
                IsNull(FieldName("FIELD1")),
                And(Seq(IsNull(FieldName("FIELD2")), EqualToValue(FieldName("FIELD3"), Value("VALUE3"))))
              )
            )
          ),
          limit = Some(100)
        )

        select.toSql shouldEqual """SELECT * FROM "TABLE1" WHERE ("FIELD1" IS NULL AND ("FIELD2" IS NULL AND "FIELD3"='VALUE3')) LIMIT 100"""
      }

      "use default writer - sort" in {
        import DefaultSqlWriters._

        val select = SimpleSelect(
          fields = AllField,
          table = TableName("TABLE1"),
          orderBy = List(SortCriteria(FieldName("name"), Direction.Ascending), SortCriteria(FieldName("age"), Direction.Descending))
        )

        select.toSql shouldEqual """SELECT * FROM "TABLE1" ORDER BY "name" ASC, "age" DESC"""
      }

      "use default writer - full" in {
        import DefaultSqlWriters._

        val select = SimpleSelect(
          fields = SpecificFields(Seq(FieldName("FIELD1"), FieldName("FIELD2"), FieldName("FIELD3"))),
          table = TableName("TABLE1"),
          where = Some(
            And(
              Seq(
                IsNull(FieldName("FIELD1")),
                And(Seq(IsNull(FieldName("FIELD2")), EqualToValue(FieldName("FIELD3"), Value("VALUE3"))))
              )
            )
          ),
          limit = Some(100),
          distinct = Some(true),
          orderBy = List(SortCriteria(FieldName("FIELD1"), Direction.Descending), SortCriteria(FieldName("FIELD2"), Direction.Ascending))
        )

        select.toSql shouldEqual """SELECT DISTINCT "FIELD1","FIELD2","FIELD3" FROM "TABLE1" WHERE ("FIELD1" IS NULL AND ("FIELD2" IS NULL AND "FIELD3"='VALUE3')) ORDER BY "FIELD1" DESC, "FIELD2" ASC LIMIT 100"""
      }
    }

  }
}
