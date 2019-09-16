package com.emarsys.rdb.connector.bigquery

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter.common
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}

import scala.concurrent.TimeoutException

trait BigQueryErrorHandling {
  protected def errorHandler: PartialFunction[Throwable, DatabaseError] = {
    case ex: TimeoutException => DatabaseError(ErrorCategory.Timeout, ErrorName.QueryTimeout, ex)
  }

  protected def eitherErrorHandler[T]: PartialFunction[Throwable, Either[DatabaseError, T]] =
    (errorHandler orElse common) andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    (errorHandler orElse common) andThen Source.failed
}
