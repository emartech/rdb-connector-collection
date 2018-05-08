package com.emarsys.rdb.connector.bigquery

import com.emarsys.rdb.connector.common.models.Errors.{ConnectionError, ConnectorError}

trait BigQueryErrorHandling {

  protected def errorHandler[T](): PartialFunction[Throwable, Either[ConnectorError,T]] = {
    case ex: Exception => Left(ConnectionError(ex))
  }

}
