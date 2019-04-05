package com.emarsys.rdb.connector.common.models

import com.emarsys.rdb.connector.common.models.Errors.{ConnectionError, TableNotFound}
import org.scalatest.{Matchers, WordSpecLike}

class ErrorsSpec extends WordSpecLike with Matchers {

  class MyException(message: String) extends Exception(message)

  "ConnectionError" should {
    "return all information from itself with toString" in {
      ConnectionError(new MyException("error1")).toString shouldEqual
        "com.emarsys.rdb.connector.common.models.Errors$ConnectionError: com.emarsys.rdb.connector.common.models.ErrorsSpec$MyException: error1"
    }
    "return all information from itself with getMessage" in {
      ConnectionError(new MyException("error1")).getMessage shouldEqual
        "com.emarsys.rdb.connector.common.models.ErrorsSpec$MyException: error1"
    }
  }

  "TableNotFound" should {
    "return all information from itself with toString" in {
      TableNotFound("table1").toString shouldEqual
        "com.emarsys.rdb.connector.common.models.Errors$TableNotFound: Table not found: table1"
    }
    "return all information from itself with getMessage" in {
      TableNotFound("table1").getMessage shouldEqual
        "Table not found: table1"
    }
  }

}
