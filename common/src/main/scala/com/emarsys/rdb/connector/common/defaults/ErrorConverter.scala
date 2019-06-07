package com.emarsys.rdb.connector.common.defaults

import java.sql.{SQLException, SQLSyntaxErrorException}
import java.util.concurrent.{RejectedExecutionException, TimeoutException}

import com.emarsys.rdb.connector.common.models.Errors._

object ErrorConverter {
  val common: PartialFunction[Throwable, ConnectorError] = {
    case e: ConnectorError             => e
    case e: RejectedExecutionException => TooManyQueries(getErrorMessage(e))
    case e: TimeoutException           => CompletionTimeout(getErrorMessage(e))
    case e                             => ErrorWithMessage(getErrorMessage(e))
  }

  val sql: PartialFunction[Throwable, ConnectorError] = {
    case e: RejectedExecutionException if e.getMessage.contains("active threads = 0") => StuckPool(getErrorMessage(e))
    case e: SQLSyntaxErrorException                                                   => SqlSyntaxError(getErrorMessage(e))
    case e: SQLException if e.getMessage.contains("Communications link failure") =>
      CommunicationsLinkFailure(getErrorMessage(e))
    case e: SQLException => ErrorWithMessage(s"[${e.getSQLState}] - [${e.getErrorCode}] - ${getErrorMessage(e)}")
  }

  val default = sql orElse common

  def getErrorMessage(ex: Throwable): String = {
    val message       = ex.getMessage
    val causeMessages = getCauseMessages(ex).filterNot(_.isEmpty)
    val allMessages   = (message :: causeMessages).distinct

    allMessages.mkString("\nCaused by: ")
  }

  def getCauseMessages(ex: Throwable): List[String] = {
    val cause = ex.getCause
    if (cause != null) {
      cause.getMessage :: getCauseMessages(cause)
    } else {
      Nil
    }
  }
}
