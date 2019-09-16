package com.emarsys.rdb.connector

import cats.data.EitherT
import com.emarsys.rdb.connector.common.models.Errors.{DatabaseError, ErrorCategory, ErrorName}

import scala.concurrent.Future

package object common {
  type ConnectorResponse[T]   = Future[Either[DatabaseError, T]]
  type ConnectorResponseET[T] = EitherT[Future, DatabaseError, T]

  def notImplementedOperation[T](message: String): ConnectorResponse[T] =
    Future.successful(
      Left(DatabaseError(ErrorCategory.FatalQueryExecution, ErrorName.NotImplementedOperation, message))
    )
}
