package com.emarsys.rdb.connector.snowflake

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import com.emarsys.rdb.connector.common.models.Errors.DatabaseError

trait SnowflakeErrorHandling {
  import ErrorConverter._

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[DatabaseError, T]] = default.andThen(Left.apply(_))

  protected def streamErrorHandler[A]: PartialFunction[Throwable, Source[A, NotUsed]] = default.andThen(Source.failed(_))
}
