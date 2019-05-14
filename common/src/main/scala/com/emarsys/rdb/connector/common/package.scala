package com.emarsys.rdb.connector

import com.emarsys.rdb.connector.common.models.Errors.{ConnectorError, NotImplementedOperation}

import scala.concurrent.Future
import scala.concurrent.duration._

package object common {
  type ConnectorResponse[T] = Future[Either[ConnectorError, T]]

  def notImplementedOperation[T](message: String): ConnectorResponse[T] =
    Future.successful(Left(NotImplementedOperation(message)))

  def completionTimeout(timeout: FiniteDuration): FiniteDuration = timeout

  def idleTimeout(timeout: FiniteDuration): FiniteDuration = {
    (scale(timeout, 0.99) min (timeout - 1.second)) max 1.second
  }

  def queryTimeout(timeout: FiniteDuration): FiniteDuration = {
    (scale(timeout, 0.98) min (timeout - 2.seconds)) max 1.second
  }

  private def scale(timeout: FiniteDuration, v: Double): FiniteDuration = {
    val scaledTimeoutNanos = timeout.toNanos * v
    scaledTimeoutNanos.nanos
  }
}
