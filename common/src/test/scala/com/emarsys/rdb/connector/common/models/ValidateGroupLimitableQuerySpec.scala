package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.SimpleSelect._
import org.scalatest.{Matchers, WordSpecLike}

class ValidateGroupLimitableQuerySpec extends WordSpecLike with Matchers {

  import ValidateGroupLimitableQuery.GroupLimitValidationResult._

  "ValidateGroupLimitableQuery" should {

    "empty where cause => Simple" in {
      val select = SimpleSelect(AllField, TableName("table"))
      ValidateGroupLimitableQuery.validate(select) shouldBe Simple
    }

    "non OR where cause => Simple" in {
      val select =
        SimpleSelect(AllField, TableName("table"), Some(And(Seq(NotNull(FieldName("a")), NotNull(FieldName("b"))))))
      ValidateGroupLimitableQuery.validate(select) shouldBe Simple
    }

    "badly optimized OR where cause => Simple" in {
      val select = SimpleSelect(AllField, TableName("table"), Some(Or(Seq(NotNull(FieldName("a"))))))
      ValidateGroupLimitableQuery.validate(select) shouldBe Simple
    }

    "all OR has the same fields => Groupable" in {
      val references = Seq("b", "a")
      val expected = Groupable(references)
      val select = SimpleSelect(
        AllField,
        TableName("table"),
        Some(
          Or(
            Seq(
              And(Seq(EqualToValue(FieldName("a"), Value("x")), EqualToValue(FieldName("b"), Value("y")))),
              And(Seq(EqualToValue(FieldName("b"), Value("z")), EqualToValue(FieldName("a"), Value("w"))))
            )
          )
        )
      )
      ValidateGroupLimitableQuery.validate(select) match {
        case Groupable(references) => references should contain theSameElementsAs(references)
        case other => fail(s"$other was not equal to $expected")
      }
    }

    "OR has no inner And => Groupable" in {
      val select = SimpleSelect(
        AllField,
        TableName("table"),
        Some(
          Or(
            Seq(
              EqualToValue(FieldName("a"), Value("x")),
              EqualToValue(FieldName("a"), Value("y"))
            )
          )
        )
      )
      ValidateGroupLimitableQuery.validate(select) shouldBe Groupable(Seq("a"))
    }

    "not all OR has the same fields => NotGroupable" in {
      val select = SimpleSelect(
        AllField,
        TableName("table"),
        Some(
          Or(
            Seq(
              And(Seq(EqualToValue(FieldName("a"), Value("x")), EqualToValue(FieldName("b"), Value("y")))),
              And(Seq(EqualToValue(FieldName("b"), Value("z")), EqualToValue(FieldName("c"), Value("w"))))
            )
          )
        )
      )
      ValidateGroupLimitableQuery.validate(select) shouldBe NotGroupable

      val select2 = SimpleSelect(
        AllField,
        TableName("table"),
        Some(
          Or(
            Seq(
              And(Seq(EqualToValue(FieldName("a"), Value("x")), EqualToValue(FieldName("b"), Value("y")))),
              And(Seq(EqualToValue(FieldName("b"), Value("z")), EqualToValue(FieldName("c"), Value("w")))),
              And(Seq(EqualToValue(FieldName("a"), Value("x")), EqualToValue(FieldName("c"), Value("w"))))
            )
          )
        )
      )

      ValidateGroupLimitableQuery.validate(select2) shouldBe NotGroupable
    }

    "OR has non EqualToValue same fields => NotGroupable" in {
      val select = SimpleSelect(
        AllField,
        TableName("table"),
        Some(
          Or(
            Seq(
              And(Seq(EqualToValue(FieldName("a"), Value("x")), NotNull(FieldName("b")))),
              And(Seq(EqualToValue(FieldName("a"), Value("z")), NotNull(FieldName("b"))))
            )
          )
        )
      )
      ValidateGroupLimitableQuery.validate(select) shouldBe NotGroupable
    }

  }
}
