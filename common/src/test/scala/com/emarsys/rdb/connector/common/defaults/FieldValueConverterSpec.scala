package com.emarsys.rdb.connector.common.defaults

import com.emarsys.rdb.connector.common.defaults.FieldValueConverter._
import com.emarsys.rdb.connector.common.models.DataManipulation.FieldValueWrapper._
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import org.scalatest.{Matchers, WordSpecLike}

class FieldValueConverterSpec extends WordSpecLike with Matchers {

  "FieldValueConverter" when {

    "StringValue" in {
      import DefaultFieldValueConverters.stringValueConverter

      StringValue("hello").toSimpleSelectValue shouldEqual Some(Value("hello"))
    }

    "IntValue" in {
      import DefaultFieldValueConverters.intValueConverter

      IntValue(777).toSimpleSelectValue shouldEqual Some(Value("777"))
    }

    "BigDecimalValue" in {
      import DefaultFieldValueConverters.bigDecimalValueConverter

      BigDecimalValue(777).toSimpleSelectValue shouldEqual Some(Value("777"))
    }

    "BooleanValue - true" in {
      import DefaultFieldValueConverters.booleanValueConverter

      BooleanValue(true).toSimpleSelectValue shouldEqual Some(Value("true"))
    }

    "BooleanValue - false" in {
      import DefaultFieldValueConverters.booleanValueConverter

      BooleanValue(false).toSimpleSelectValue shouldEqual Some(Value("false"))
    }

    "NullValue" in {
      import DefaultFieldValueConverters.nullValueConverter

      NullValue.toSimpleSelectValue shouldEqual None
    }
  }
}
