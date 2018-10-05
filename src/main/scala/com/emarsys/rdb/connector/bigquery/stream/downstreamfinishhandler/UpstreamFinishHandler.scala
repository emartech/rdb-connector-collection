package com.emarsys.rdb.connector.bigquery.stream.downstreamfinishhandler

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

case class UpstreamFinishHandler[T](callBack: T => Unit) extends GraphStage[FlowShape[T, T]] {


  val in = Inlet[T]("TimeoutHandler.in")
  val out = Outlet[T]("TimeoutHandler.out")

  override val shape = FlowShape.of(in, out)

  var firstData: Option[T] = None

  override def createLogic(attr: Attributes): GraphStageLogic = new GraphStageLogic(shape) {

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        val element = grab(in)
        firstData = Some(element)
        push(out, element)
      }

      override def onUpstreamFinish() = {
        firstData.foreach(callBack)
        super.onUpstreamFinish()
      }
    })

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        pull(in)
      }
    })
  }
}
