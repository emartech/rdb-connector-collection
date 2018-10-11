package com.emarsys.rdb.connector

import java.util.UUID

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import com.emarsys.rdb.connector.common.ConnectorResponse
import org.scalatest.Matchers

import scala.concurrent.Await
import scala.concurrent.duration.Duration

package object test extends Matchers {
  def uuidGenerate = UUID.randomUUID().toString.replace("-","")

  def checkResultWithoutRowOrder(result: Seq[Seq[String]], expected: Seq[Seq[String]]): Unit = {
    result.size shouldEqual expected.size
    result.head.map(_.toUpperCase) shouldEqual expected.head.map(_.toUpperCase)
    if(result.size > 1) {
      result.tail should contain allElementsOf expected.tail
      expected.tail should contain allElementsOf result.tail
    }
  }

  def getConnectorResult(connRes: ConnectorResponse[Source[Seq[String], NotUsed]], awaitTimeout: Duration)(implicit mat: Materializer): Seq[Seq[String]] = {
    val resultE = Await.result(connRes, awaitTimeout)

    resultE shouldBe a[Right[_, _]]
    val resultStream: Source[Seq[String], NotUsed] = resultE.right.get

    Await.result(resultStream.runWith(Sink.seq), awaitTimeout)
  }
}
