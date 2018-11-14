package com.emarsys.rdb.connector.bigquery.util

import akka.http.scaladsl.model.{HttpEntity, Uri}
import akka.stream.Materializer
import akka.util.ByteString

import scala.concurrent.{ExecutionContext, Future}

object AkkaHttpPimps {

  implicit class HttpEntityExtension(entity: HttpEntity) {
    def convertToString()(implicit materializer: Materializer, ec: ExecutionContext): Future[String] =
      entity.dataBytes
        .runFold(ByteString(""))(_ ++ _)
        .map(_.utf8String)
  }

  implicit class UriPathExtension(path: Uri.Path) {
    def ?/(segment: String): Uri.Path = {
      if (path.endsWithSlash) {
        path + segment
      } else {
        path / segment
      }
    }
  }

}
