package com.emarsys.rdb.connector.common.defaults

import java.sql.{SQLException, SQLSyntaxErrorException, SQLTransientConnectionException}
import java.util.concurrent.{RejectedExecutionException, TimeoutException}

import cats.data.Chain
import com.emarsys.rdb.connector.common.models.Errors._

import scala.annotation.tailrec

object ErrorConverter {
  val common: PartialFunction[Throwable, DatabaseError] = {
    case e: DatabaseError              => e
    case e: RejectedExecutionException => DatabaseError(ErrorCategory.RateLimit, ErrorName.TooManyQueries, e)
    case e: TimeoutException           => DatabaseError(ErrorCategory.Timeout, ErrorName.CompletionTimeout, e)
    case e                             => DatabaseError(ErrorCategory.Unknown, ErrorName.Unknown, e)
  }

  val sql: PartialFunction[Throwable, DatabaseError] = {
    case e: RejectedExecutionException if e.getMessage.contains("active threads = 0") =>
      DatabaseError(ErrorCategory.RateLimit, ErrorName.StuckPool, e)
    case e: SQLSyntaxErrorException =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.SqlSyntaxError, e)
    case e: SQLException if e.getMessage.contains("Communications link failure") =>
      DatabaseError(ErrorCategory.Transient, ErrorName.CommunicationsLinkFailure, e)
    case e: SQLTransientConnectionException if e.getMessage.contains("timed out") =>
      DatabaseError(ErrorCategory.Timeout, ErrorName.ConnectionTimeout, e)
    case e: SQLException =>
      DatabaseError(
        ErrorCategory.Unknown,
        ErrorName.Unknown,
        s"[${e.getSQLState}] - [${e.getErrorCode}] - ${e.getMessage}",
        Some(e),
        None
      )
  }

  val default: PartialFunction[Throwable, DatabaseError] = sql orElse common

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
