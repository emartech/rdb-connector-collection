package com.emarsys.rdb.connector.test.util

import org.scalactic.source
import org.scalatest.exceptions.{StackDepthException, TestFailedException}
import org.scalatest.{EitherValues => ScalatestEitherValues}

trait EitherValues extends ScalatestEitherValues {

  implicit def convertEitherToValuable[L, R](either: Either[L, R])(
      implicit pos: source.Position
  ): EitherValuable[L, R] = new EitherValuable(either, pos)

  class EitherValuable[L, R](either: Either[L, R], pos: source.Position) {

    def value: R = {
      either match {
        case Left(_) =>
          throw new TestFailedException(
            (_: StackDepthException) => Some("The Either value is not a Right(_)"),
            None,
            pos
          )
        case Right(value) => value
      }
    }
  }

}
