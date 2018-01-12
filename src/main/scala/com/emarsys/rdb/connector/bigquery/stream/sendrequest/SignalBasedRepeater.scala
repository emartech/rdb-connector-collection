package com.emarsys.rdb.connector.bigquery.stream.sendrequest

import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.stream.{Attributes, FanInShape2, Inlet, Outlet}

class SignalBasedRepeater[T]() extends GraphStage[FanInShape2[T, Boolean, (T, Boolean)]] {
  private val in = Inlet[T]("SignalBasedRepeater.in")
  private val signal = Inlet[Boolean]("SignalBasedRepeater.signal")
  private val out = Outlet[(T, Boolean)]("SignalBasedRepeater.out")

  val shape = new FanInShape2(in, signal, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
    new GraphStageLogic(shape) {

      var isRetrying: Option[Boolean] = None
      var lastElem: Option[T] = None

      override def preStart(): Unit = {
        pull(in)
        pull(signal)
      }

      setHandler(signal, new InHandler {
        override def onPush(): Unit = {
          if (isRetrying.isDefined) throw new IllegalStateException("Retry request received while already retrying")

          val successfulLast: Boolean = grabAndPull(signal)
          isRetrying = Some(!successfulLast)

          if (!isClosed(in)) {
            produceElementIfReady()
          } else {
            retryOrCompleteIfReady()
          }
        }
      })

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          produceElementIfReady()
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = {
          if (!isClosed(in)) {
            produceElementIfReady()
          } else {
            retryOrCompleteIfReady()
          }
        }
      })

      def produceElementIfReady(): Unit = {
        if (isAvailable(out)) {
          if (isRetrying.contains(false) && isAvailable(in)) {
            propagateInputToOutput()
            isRetrying = None
          } else if (isRetrying.contains(true)) {
            retry()
            isRetrying = None
          }
        }
      }

      private def propagateInputToOutput() = {
        val element = grabAndPull(in)
        lastElem = Some(element)
        push(out, (element, false))
      }

      def retryOrCompleteIfReady(): Unit = {
        if (isAvailable(out)) {
          if (isRetrying.contains(false)) {
            completeStage()
          } else if (isRetrying.contains(true)) {
            retry()
            isRetrying = None
          }
        }
      }


      private def grabAndPull[T](inlet: Inlet[T]): T = {
        val inputElement = grab(inlet)
        pull(inlet)

        inputElement
      }

      private def retry(): Unit = {
        if (lastElem.isEmpty) throw new IllegalStateException("First signal must be true")
        push(out, (lastElem.get, true))
      }
    }
  }
}
