package com.emarsys.rdb.connector.mysql.hack

import slick.jdbc.JdbcDataSource

import java.sql.Connection

class RouterJdbcDataSource(val jdbcDataSource: JdbcDataSource) extends JdbcDataSource {
  override def createConnection(): Connection = new RouterConnection(jdbcDataSource.createConnection())

  override def close(): Unit = jdbcDataSource.close()

  override val maxConnections: Option[Int] = jdbcDataSource.maxConnections
}
