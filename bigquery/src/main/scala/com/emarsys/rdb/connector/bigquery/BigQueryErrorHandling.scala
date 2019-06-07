package com.emarsys.rdb.connector.bigquery

import java.util.concurrent.RejectedExecutionException

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import com.emarsys.rdb.connector.common.models.Errors._

import scala.concurrent.TimeoutException

trait BigQueryErrorHandling {
  import ErrorConverter._

  protected def errorHandler: PartialFunction[Throwable, ConnectorError] = {
    case ex: TimeoutException => QueryTimeout(getErrorMessage(ex))
  }

  protected def eitherErrorHandler[T]: PartialFunction[Throwable, Either[ConnectorError, T]] =
    (errorHandler orElse ErrorConverter.common) andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    (errorHandler orElse ErrorConverter.common) andThen Source.failed
}
