package com.emarsys.rdb.connector.bigquery.stream.pagetoken

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.emarsys.rdb.connector.bigquery.stream.parser.PagingInfo

object EndOfStreamDetector {

  def apply(): Flow[(Boolean, PagingInfo), (Boolean, PagingInfo), NotUsed] = {
    Flow[(Boolean, PagingInfo)].takeWhile {
      case (retry, pagingInfo) => retry || pagingInfo.pageToken.isDefined
    }
  }

}
