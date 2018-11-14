package com.emarsys.rdb.connector.utils
import akka.stream.scaladsl.Source
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import org.mockito.Mockito.when

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

object TestHelper {

  def selectUniqueValueMock(select: SimpleSelect, connector: Connector, timeout: FiniteDuration)(implicit ec: ExecutionContext) = {
    when(connector.simpleSelect(select, timeout)).thenReturn(Future(Right(Source(List(Seq("columnName"),Seq("value"))))))
  }

  def selectExactNumberMock(number: Int, tableName: String, connector: Connector, timeout: FiniteDuration)(implicit ec: ExecutionContext) = {
    when(connector.simpleSelect(SimpleSelect(AllField, TableName(tableName),
      where = Some(
        Or(Seq(EqualToValue(FieldName("A1"), Value(number.toString)), NotNull(FieldName("A1")))))
      ), timeout
    ))
      .thenReturn(Future(Right(Source(List.fill(number)(Seq("foo"))))))
  }

  def emptyResultSelectMock(select: SimpleSelect, connector: Connector, timeout: FiniteDuration)(implicit ec: ExecutionContext) = {
    when(connector.simpleSelect(select, timeout)
    ).thenReturn(Future(Right(Source(List.empty))))
  }
}
