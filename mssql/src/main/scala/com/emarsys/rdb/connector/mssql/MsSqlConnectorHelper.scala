package com.emarsys.rdb.connector.mssql

import slick.jdbc.SQLServerProfile.api._

import scala.concurrent.{ExecutionContext, Future}

trait MsSqlConnectorHelper {
  import cats.syntax.functor._
  import cats.instances.future._

  protected def checkConnection(db: Database)(implicit executionContext: ExecutionContext): Future[Unit] = {
    db.run(sql"SELECT 1".as[(String)]).void
  }

  private[mssql] def createUrl(host: String, port: Int, database: String, connectionParams: String): String = {
    s"jdbc:sqlserver://$host:$port;databaseName=$database${safeConnectionParams(connectionParams)}"
  }

  private[mssql] def isSslDisabledOrTamperedWith(connectionParams: String): Boolean = {
    connectionParams.matches(".*encrypt=false.*") ||
    connectionParams.matches(".*trustServerCertificate=false.*") ||
    connectionParams.matches(".*trustStore=.*")
  }

  private[mssql] def safeConnectionParams(connectionParams: String): String = {
    if (connectionParams.startsWith(";") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s";$connectionParams"
    }
  }
}
