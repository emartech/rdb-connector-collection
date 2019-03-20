package com.emarsys.rdb.connector.common.defaults

import java.sql.{SQLException, SQLSyntaxErrorException}
import java.util.concurrent.{RejectedExecutionException, TimeoutException}

import com.emarsys.rdb.connector.common.models.Errors._

object ErrorConverter {
  val common: PartialFunction[Throwable, ConnectorError] = {
    case e: ConnectorError             => e
    case e: RejectedExecutionException => TooManyQueries(e.getMessage)
    case e: TimeoutException           => CompletionTimeout(e.getMessage)
    case e                             => ErrorWithMessage(e.toString)
  }

  val sql: PartialFunction[Throwable, ConnectorError] = {
    case e: RejectedExecutionException if e.getMessage.contains("active threads = 0") => StuckPool(e.getMessage)
    case e: SQLSyntaxErrorException                                                   => SqlSyntaxError(e.getMessage)
    case e: SQLException if e.getMessage.contains("Communications link failure") =>
      CommunicationsLinkFailure(e.getMessage)
    case e: SQLException => ErrorWithMessage(s"[${e.getSQLState}] - ${e.getMessage}")
  }

  val default = sql orElse common
}
