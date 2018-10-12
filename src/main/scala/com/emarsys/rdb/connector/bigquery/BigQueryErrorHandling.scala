package com.emarsys.rdb.connector.bigquery

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.models.Errors.{ConnectionError, ConnectorError, QueryTimeout}

import scala.concurrent.TimeoutException

trait BigQueryErrorHandling {

  protected def errorHandler: PartialFunction[Throwable, ConnectorError] = {
    case ce: ConnectorError   => ce
    case to: TimeoutException => QueryTimeout(to.getMessage)
    case ex: Exception        => ConnectionError(ex)
  }

  protected def eitherErrorHandler[T]: PartialFunction[Throwable, Either[ConnectorError, T]] =
    errorHandler andThen Left.apply

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] =
    errorHandler andThen Source.failed
}
