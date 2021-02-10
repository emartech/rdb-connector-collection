package com.emarsys.rdb.connector.hana

import com.emarsys.rdb.connector.common.defaults.ErrorConverter
import com.emarsys.rdb.connector.common.models.Errors.DatabaseError

trait HanaErrorHandling {
  import ErrorConverter._

  protected def eitherErrorHandler[T](): PartialFunction[Throwable, Either[DatabaseError, T]] =
    default.andThen(Left.apply(_))
}
