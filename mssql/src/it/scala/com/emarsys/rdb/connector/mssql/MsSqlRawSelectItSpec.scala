package com.emarsys.rdb.connector.mssql

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.emarsys.rdb.connector.mssql.utils.SelectDbInitHelper
import com.emarsys.rdb.connector.test._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.concurrent.ExecutionContextExecutor

class MsSqlRawSelectItSpec
    extends TestKit(ActorSystem("MsSqlRawSelectItSpec"))
    with RawSelectItSpec
    with SelectDbInitHelper
    with AnyWordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def afterAll(): Unit = {
    shutdown()
    cleanUpDb()
    connector.close()
  }

  override def beforeAll(): Unit = {
    initDb()
  }

  override val simpleSelect            = s"SELECT * FROM [$aTableName];"
  override val badSimpleSelect         = s"SELECT * ForM [$aTableName]"
  override val simpleSelectNoSemicolon = s"""SELECT * FROM [$aTableName]"""

  "#analyzeRawSelect" should {
    "return result" in {
      val result = getConnectorResult(connector.analyzeRawSelect(simpleSelect), awaitTimeout)

      result.size shouldEqual 2
      result.head shouldEqual Seq("Microsoft SQL Server 2005 XML Showplan")
      result(1).head.startsWith("""<ShowPlanXML""") shouldBe true
    }
  }
}
