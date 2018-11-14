package com.emarsys.rdb.connector

import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, NotImplementedOperation}

import scala.concurrent.Future

package object common {
  type ConnectorResponse[T] = Future[Either[ConnectorError,T]]

  def notImplementedOperation[T](message: String): ConnectorResponse[T] = Future.successful(Left(NotImplementedOperation(message)))
}
