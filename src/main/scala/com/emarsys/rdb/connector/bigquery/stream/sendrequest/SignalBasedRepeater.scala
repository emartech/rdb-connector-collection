package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FanInShape2, Inlet, Outlet}

class SignalBasedRepeater[T]() extends GraphStage[FanInShape2[T, Unit, (T, Boolean)]] {
  private val in = Inlet[T]("SignalBasedRepeater.in")
  private val signal = Inlet[Unit]("SignalBasedRepeater.signal")
  private val out = Outlet[(T, Boolean)]("SignalBasedRepeater.out")

  val shape = new FanInShape2(in, signal, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) {

      var lastElem: Option[T] = None
      var errorFlag = false

      override def preStart(): Unit = {
        pull(signal)
      }

      setHandler(signal, new InHandler {

        override def onPush(): Unit = {
          if (lastElem.isEmpty) throw new IllegalStateException("Must receive first element before got signal")
          if (errorFlag) throw new IllegalStateException("Got multiple signal")
          errorFlag = true
          pull(signal)
        }
      })

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          if (errorFlag) throw new IllegalStateException("We only pulled when flag was false")
          lastElem = Some(grab(in))
          push(out, (lastElem.get, false))
        }

        override def onUpstreamFinish(): Unit = {
          complete(out)
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (errorFlag) {
            errorFlag = false
            push(out, (lastElem.get, true))
          } else {
            pull(in)
          }
        }
      })

    }
  }
}
