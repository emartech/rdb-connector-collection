package com.emarsys.rdb.connector.bigquery.stream

import akka.NotUsed
import akka.http.scaladsl.model.HttpResponse
import akka.stream.scaladsl.Flow

object ErrorSignalProcessor {
  def apply(): Flow[HttpResponse, Unit, NotUsed] = {
    Flow[HttpResponse].filter(response => {
      response.status.intValue() == 401
    }).map[Unit](_ => ())
  }
}
