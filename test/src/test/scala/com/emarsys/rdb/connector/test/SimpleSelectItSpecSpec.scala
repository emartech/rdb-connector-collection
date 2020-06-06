package com.emarsys.rdb.connector.test

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.Source
import akka.testkit.TestKit
import com.emarsys.rdb.connector.common.models.{Connector, SimpleSelect}
import com.emarsys.rdb.connector.common.models.SimpleSelect._
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.mockito.MockitoSugar

import scala.collection.immutable //TODO remove this after 2.13
import scala.concurrent.Future

class SimpleSelectItSpecSpec
    extends TestKit(ActorSystem("SimpleSelectItSpecSpec"))
    with SimpleSelectItSpec
    with MockitoSugar
    with BeforeAndAfterAll {

  implicit val materializer: Materializer = ActorMaterializer()

  implicit val executionContext = system.dispatcher

  override val connector = mock[Connector]

  override def initDb(): Unit = ()

  override def cleanUpDb(): Unit = ()

  override def afterAll() = {
    shutdown()
  }

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(aTableName)), queryTimeout)).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v1", "1", "1"),
            immutable.Seq("v3", "3", "1"),
            immutable.Seq("v2", "2", "0"),
            immutable.Seq("v6", "6", null),
            immutable.Seq("v4", "-4", "0"),
            immutable.Seq("v5", null, "0"),
            immutable.Seq("v7", null, null)
          )
        )
      )
    )
  )

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(bTableName)), queryTimeout)).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("B1", "B2", "B3", "B4"),
            immutable.Seq("b!3", "b@3", "b#3", null),
            immutable.Seq("b;2", "b\\2", "b'2", "b=2"),
            immutable.Seq("b,1", "b.1", "b:1", "b\"1"),
            immutable.Seq("b$4", "b%4", "b 4", null)
          )
        )
      )
    )
  )

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(cTableName)), queryTimeout)).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("C"),
            immutable.Seq("c12"),
            immutable.Seq("c12"),
            immutable.Seq("c3")
          )
        )
      )
    )
  )

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(cTableName), distinct = Some(true)), queryTimeout))
    .thenReturn(
      Future(
        Right(
          Source(
            immutable.Seq(
              immutable.Seq("C"),
              immutable.Seq("c12"),
              immutable.Seq("c3")
            )
          )
        )
      )
    )

  when(
    connector.simpleSelect(
      SimpleSelect(SpecificFields(immutable.Seq(FieldName("A1"), FieldName("A3"))), TableName(aTableName)),
      queryTimeout
    )
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A3"),
            immutable.Seq("v1", "1"),
            immutable.Seq("v2", "0"),
            immutable.Seq("v6", null),
            immutable.Seq("v3", "1"),
            immutable.Seq("v4", "0"),
            immutable.Seq("v5", "0"),
            immutable.Seq("v7", null)
          )
        )
      )
    )
  )

  when(connector.simpleSelect(SimpleSelect(AllField, TableName(aTableName), limit = Some(2)), queryTimeout)).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v3", "3", "1"),
            immutable.Seq("v4", "-4", "0")
          )
        )
      )
    )
  )

  when(
    connector
      .simpleSelect(SimpleSelect(AllField, TableName(aTableName), where = Some(IsNull(FieldName("A2")))), queryTimeout)
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v5", null, "0"),
            immutable.Seq("v7", null, null)
          )
        )
      )
    )
  )

  when(
    connector
      .simpleSelect(SimpleSelect(AllField, TableName(aTableName), where = Some(NotNull(FieldName("A2")))), queryTimeout)
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v2", "2", "0"),
            immutable.Seq("v4", "-4", "0"),
            immutable.Seq("v3", "3", "1"),
            immutable.Seq("v1", "1", "1"),
            immutable.Seq("v6", "6", null)
          )
        )
      )
    )
  )

  when(
    connector.simpleSelect(
      SimpleSelect(AllField, TableName(aTableName), where = Some(EqualToValue(FieldName("A1"), Value("v3")))),
      queryTimeout
    )
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v3", "3", "1")
          )
        )
      )
    )
  )

  val whereCondition = Or(
    List(
      EqualToValue(FieldName("B4"), Value("b\"1")),
      And(
        List(
          EqualToValue(FieldName("B2"), Value("b\\2")),
          EqualToValue(FieldName("B3"), Value("b'2"))
        )
      )
    )
  )
  when(
    connector.simpleSelect(SimpleSelect(AllField, TableName(bTableName), where = Some(whereCondition)), queryTimeout)
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("B1", "B2", "B3", "B4"),
            immutable.Seq("b,1", "b.1", "b:1", "b\"1"),
            immutable.Seq("b;2", "b\\2", "b'2", "b=2")
          )
        )
      )
    )
  )

  when(
    connector.simpleSelect(
      SimpleSelect(AllField, TableName(aTableName), where = Some(EqualToValue(FieldName("A2"), Value("3")))),
      queryTimeout
    )
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v3", "3", "1")
          )
        )
      )
    )
  )

  when(
    connector.simpleSelect(
      SimpleSelect(AllField, TableName(aTableName), where = Some(EqualToValue(FieldName("A3"), Value("1")))),
      queryTimeout
    )
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v1", "1", "1"),
            immutable.Seq("v3", "3", "1")
          )
        )
      )
    )
  )

  when(
    connector.simpleSelect(
      SimpleSelect(
        AllField,
        TableName(aTableName),
        where = Some(
          Or(
            immutable.Seq(
              EqualToValue(FieldName("A1"), Value("v1")),
              EqualToValue(FieldName("A1"), Value("v2")),
              IsNull(FieldName("A2"))
            )
          )
        )
      ),
      queryTimeout
    )
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v1", "1", "1"),
            immutable.Seq("v5", null, "0"),
            immutable.Seq("v2", "2", "0"),
            immutable.Seq("v7", null, null)
          )
        )
      )
    )
  )

  when(
    connector.simpleSelect(
      SimpleSelect(
        AllField,
        TableName(aTableName),
        where = Some(
          And(
            immutable.Seq(
              EqualToValue(FieldName("A1"), Value("v7")),
              IsNull(FieldName("A2"))
            )
          )
        )
      ),
      queryTimeout
    )
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v7", null, null)
          )
        )
      )
    )
  )

  when(
    connector.simpleSelect(
      SimpleSelect(
        AllField,
        TableName(aTableName),
        where = Some(
          And(
            immutable.Seq(
              EqualToValue(FieldName("A1"), Value("v7")),
              NotNull(FieldName("A2"))
            )
          )
        )
      ),
      queryTimeout
    )
  ).thenReturn(Future(Right(Source.empty)))

  when(
    connector.simpleSelect(
      SimpleSelect(
        AllField,
        TableName(aTableName),
        where = Some(
          Or(
            immutable.Seq(
              EqualToValue(FieldName("A1"), Value("v1")),
              And(
                immutable.Seq(
                  IsNull(FieldName("A2")),
                  IsNull(FieldName("A3"))
                )
              )
            )
          )
        )
      ),
      queryTimeout
    )
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v1", "1", "1"),
            immutable.Seq("v7", null, null)
          )
        )
      )
    )
  )

  when(
    connector.simpleSelect(
      SimpleSelect(
        AllField,
        TableName(aTableName),
        orderBy = List(SortCriteria(FieldName("A1"), Direction.Descending))
      ),
      queryTimeout
    )
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v7", null, null),
            immutable.Seq("v6", "6", null),
            immutable.Seq("v5", null, "0"),
            immutable.Seq("v4", "-4", "0"),
            immutable.Seq("v3", "3", "1"),
            immutable.Seq("v2", "2", "0"),
            immutable.Seq("v1", "1", "1")
          )
        )
      )
    )
  )

  when(
    connector.simpleSelect(
      SimpleSelect(
        AllField,
        TableName(aTableName),
        orderBy = List(SortCriteria(FieldName("A1"), Direction.Ascending))
      ),
      queryTimeout
    )
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v1", "1", "1"),
            immutable.Seq("v2", "2", "0"),
            immutable.Seq("v3", "3", "1"),
            immutable.Seq("v4", "-4", "0"),
            immutable.Seq("v5", null, "0"),
            immutable.Seq("v6", "6", null),
            immutable.Seq("v7", null, null)
          )
        )
      )
    )
  )

  when(
    connector.simpleSelect(
      SimpleSelect(
        AllField,
        TableName(aTableName),
        where = Some(NotNull(FieldName("A3"))),
        orderBy =
          List(SortCriteria(FieldName("A3"), Direction.Ascending), SortCriteria(FieldName("A1"), Direction.Descending))
      ),
      queryTimeout
    )
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A1", "A2", "A3"),
            immutable.Seq("v5", null, "0"),
            immutable.Seq("v4", "-4", "0"),
            immutable.Seq("v2", "2", "0"),
            immutable.Seq("v3", "3", "1"),
            immutable.Seq("v1", "1", "1")
          )
        )
      )
    )
  )

  when(
    connector.simpleSelect(
      SimpleSelect(
        SpecificFields("A2"),
        TableName(aTableName),
        orderBy = List(SortCriteria(FieldName("A1"), Direction.Descending)),
        distinct = Some(true)
      ),
      queryTimeout
    )
  ).thenReturn(
    Future(
      Right(
        Source(
          immutable.Seq(
            immutable.Seq("A2", "A1"),
            immutable.Seq(null, "v7"),
            immutable.Seq("6", "v6"),
            immutable.Seq(null, "v5"),
            immutable.Seq("-4", "v4"),
            immutable.Seq("3", "v3"),
            immutable.Seq("2", "v2"),
            immutable.Seq("1", "v1")
          )
        )
      )
    )
  )

}
