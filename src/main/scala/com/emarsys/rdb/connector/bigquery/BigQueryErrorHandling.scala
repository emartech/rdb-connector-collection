package com.emarsys.rdb.connector.bigquery

import java.util.concurrent.RejectedExecutionException

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, ErrorWithMessage, QueryTimeout, TooManyQueries}

import scala.concurrent.TimeoutException

trait BigQueryErrorHandling {

  protected def errorHandler: PartialFunction[Throwable, ConnectorError] = {
    case ce: ConnectorError            => ce
    case to: TimeoutException          => QueryTimeout(to.getMessage)
    case _: RejectedExecutionException => TooManyQueries
    case ex: Exception                 => ErrorWithMessage(ex.toString)
  }

  protected def eitherErrorHandler[T]: PartialFunction[Throwable, Either[ConnectorError, T]] =
    errorHandler andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler andThen Source.failed
}
