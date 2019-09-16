package com.emarsys.rdb.connector.bigquery.stream.pagetoken

import java.net.URLEncoder

import akka.NotUsed
import akka.http.scaladsl.model.{HttpEntity, HttpMethods, HttpRequest, Uri}
import akka.stream.scaladsl.Flow
import com.emarsys.rdb.connector.bigquery.stream.parser.PagingInfo

object AddPageToken {
  def apply(): Flow[(HttpRequest, (Boolean, PagingInfo)), HttpRequest, NotUsed] = {
    Flow[(HttpRequest, (Boolean, PagingInfo))].map {
      case (request, (retry, pagingInfo)) =>
        val req = if (!retry) {
          addPageToken(request, pagingInfo)
        } else {
          request
        }

        pagingInfo.jobId match {
          case Some(id) =>
            val getReq = convertRequestToGet(req)
            addPath(getReq, id)
          case None => req
        }
    }
  }

  private def addPath(getReq: HttpRequest, path: String) =
    getReq.copy(uri = getReq.uri.withPath(getReq.uri.path ?/ path))

  private def convertRequestToGet(req: HttpRequest) = req.copy(method = HttpMethods.GET, entity = HttpEntity.Empty)

  private def addPageToken(request: HttpRequest, pagingInfo: PagingInfo) = request.copy(
    uri = request.uri
      .withQuery(Uri.Query(pagingInfo.pageToken.map(token => s"pageToken=${URLEncoder.encode(token, "utf-8")}")))
  )
}
