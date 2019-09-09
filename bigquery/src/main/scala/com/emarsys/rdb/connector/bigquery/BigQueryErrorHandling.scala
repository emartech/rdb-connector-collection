package com.emarsys.rdb.connector.bigquery

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter.commonDBError
import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, DatabaseError, ErrorCategory, ErrorName}

import scala.concurrent.TimeoutException

trait BigQueryErrorHandling {
  protected def errorHandler: PartialFunction[Throwable, ConnectorError] = {
    case ex: TimeoutException => DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, ex)
  }

  protected def eitherErrorHandler[T]: PartialFunction[Throwable, Either[ConnectorError, T]] =
    (errorHandler orElse commonDBError) andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    (errorHandler orElse commonDBError) andThen Source.failed
}
