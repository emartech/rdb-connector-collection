package com.emarsys.rdb.connector.mysql.hack

import scala.concurrent.{Future, Promise}

class QueryExecutionTracker(private val query: String) {
  private var queued: Long   = _
  private var started: Long  = _
  private var executed: Long = _
  private var ended: Long    = _

  private val promise = Promise[QueryStats]()
  val stats: Future[QueryStats] = promise.future

  def queued(ts: Long): Unit   = queued = ts
  def started(ts: Long): Unit  = started = ts
  def executed(ts: Long): Unit = executed = ts
  def ended(ts: Long): Unit    = {
    ended = ts
    promise.success(QueryStats(query, queued, started, executed, ended))
  }
}

case class QueryStats(
    query: String,
    queued: Long,
    started: Long,
    executed: Long,
    ended: Long
) {
  def queueTime()     = started - queued
  def executionTime() = executed - started
  def dbTime()        = ended - started
  def totalTime()     = ended - queued

  override def toString: String =
    s"""=== *** ===
       |Query: $query
       |
       |Times:
       |Queued:   $queued
       |Started:  $started
       |Executed: $executed
       |Ended:    $ended
       |
       |Time spent in queue: ${queueTime() / 1e6f}ms
       |Time spent executing query: ${executionTime() / 1e6f}ms
       |Time spent pestering db: ${dbTime() / 1e6f}ms
       |Total time: ${totalTime() / 1e6f}ms
       |""".stripMargin
}
