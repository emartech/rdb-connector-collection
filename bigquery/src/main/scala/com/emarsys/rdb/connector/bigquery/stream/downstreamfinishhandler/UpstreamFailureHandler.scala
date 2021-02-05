package com.emarsys.rdb.connector.bigquery.stream.downstreamfinishhandler

import java.util.concurrent.TimeoutException

import akka.stream._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}

case class UpstreamFailureHandler[T](callBack: T => Unit) extends GraphStage[FlowShape[T, T]] {

  val in = Inlet[T]("TimeoutHandler.in")
  val out = Outlet[T]("TimeoutHandler.out")

  override val shape = FlowShape.of(in, out)

  override def createLogic(attr: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    private var lastData: Option[T] = None

    setHandler(
      in,
      new InHandler {
        override def onPush(): Unit = {
          val element = grab(in)
          lastData = Some(element)
          push(out, element)
        }

        override def onUpstreamFailure(ex: Throwable) = {
          ex match {
            case _: TimeoutException => lastData.foreach(callBack)
            case _ =>
          }
          super.onUpstreamFailure(ex)
        }
      }
    )

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        pull(in)
      }

      override def onDownstreamFinish(cause: Throwable): Unit = {
        cause match {
          case _: TimeoutException => lastData.foreach(callBack)
          case _ =>
        }
        super.onDownstreamFinish(cause)
      }
    })
  }
}
