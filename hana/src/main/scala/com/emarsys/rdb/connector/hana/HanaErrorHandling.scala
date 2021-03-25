package com.emarsys.rdb.connector.hana

import java.sql.SQLException

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}
import com.sap.db.jdbc.exceptions.SQLNonTransientConnectionExceptionSapDB

trait HanaErrorHandling {
  import ErrorConverter._

  final private val INVALID_TABLE_NAME                 = 259
  final private val GENERAL_ERROR                      = 2
  final private val FATAL_ERROR                        = 3
  final private val INVALID_LICENSE                    = 19
  final private val EXCEED_MAX_CONCURRENT_TRANSACTIONS = 142

  private def errorHandler: PartialFunction[Throwable, DatabaseError] = {
    case ex: SQLException if ex.getErrorCode == INVALID_TABLE_NAME =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.TableNotFound, ex)
    case ex: SQLException if ex.getErrorCode == GENERAL_ERROR =>
      DatabaseError(ErrorCategory.Transient, ErrorName.TransientDbError, ex)
    case ex: SQLException if ex.getErrorCode == FATAL_ERROR =>
      DatabaseError(ErrorCategory.Transient, ErrorName.TransientDbError, ex)
    case ex: SQLException if ex.getErrorCode == INVALID_LICENSE =>
      DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.QueryRejected, ex)
    case ex: SQLException if ex.getErrorCode == EXCEED_MAX_CONCURRENT_TRANSACTIONS =>
      DatabaseError(ErrorCategory.RateLimit, ErrorName.TooManyQueries, ex)
    case ex: SQLNonTransientConnectionExceptionSapDB
        if ex.getSQLState == "08006" && ex.getMessage == "Data receive failed [null]." =>
      DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, ex)
  }

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[DatabaseError, T]] =
    errorHandler.orElse(default).andThen(Left.apply(_))

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler.orElse(default).andThen(Source.failed(_))
}
