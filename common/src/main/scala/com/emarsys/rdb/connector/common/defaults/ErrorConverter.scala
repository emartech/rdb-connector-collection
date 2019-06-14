package com.emarsys.rdb.connector.common.defaults

import java.sql.{SQLException, SQLSyntaxErrorException}
import java.util.concurrent.{RejectedExecutionException, TimeoutException}

import cats.data.Chain
import com.emarsys.rdb.connector.common.models.Errors._

import scala.annotation.tailrec

object ErrorConverter {
  val common: PartialFunction[Throwable, ConnectorError] = {
    case e: ConnectorError             => e
    case e: RejectedExecutionException => TooManyQueries(getErrorMessage(e)).withCause(e)
    case e: TimeoutException           => CompletionTimeout(getErrorMessage(e)).withCause(e)
    case e                             => ErrorWithMessage(getErrorMessage(e)).withCause(e)
  }

  val sql: PartialFunction[Throwable, ConnectorError] = {
    case e: RejectedExecutionException if e.getMessage.contains("active threads = 0") =>
      StuckPool(getErrorMessage(e)).withCause(e)
    case e: SQLSyntaxErrorException =>
      SqlSyntaxError(getErrorMessage(e)).withCause(e)
    case e: SQLException if e.getMessage.contains("Communications link failure") =>
      CommunicationsLinkFailure(getErrorMessage(e)).withCause(e)
    case e: SQLException =>
      ErrorWithMessage(s"[${e.getSQLState}] - [${e.getErrorCode}] - ${getErrorMessage(e)}").withCause(e)
  }

  val default = sql orElse common

  def getErrorMessage(ex: Throwable): String = {
    val message       = ex.getMessage
    val causeMessages = getCauseMessages(ex).filterNot(_.isEmpty)
    val allMessages   = (message :: causeMessages).distinct

    allMessages.mkString("\nCaused by: ")
  }

  def getCauseMessages(ex: Throwable): List[String] = {
    @tailrec
    def impl(currentEx: Throwable, seenErrors: Set[Throwable], messages: Chain[String]): Chain[String] = {
      val cause = currentEx.getCause
      if (cause != null && !seenErrors.contains(cause)) {
        val msg              = cause.getMessage
        val expandedMessages = if (msg != null) messages :+ msg else messages
        impl(cause, seenErrors + cause, expandedMessages)
      } else {
        messages
      }
    }

    impl(ex, Set.empty, Chain.empty).toList
  }
}
