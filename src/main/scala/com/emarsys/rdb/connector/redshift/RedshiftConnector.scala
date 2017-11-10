package com.emarsys.rdb.connector.redshift

import java.util.Properties

import com.emarsys.rdb.connector.common.ConnectorResponse
import com.emarsys.rdb.connector.common.models.{ConnectionConfig, Connector}
import com.emarsys.rdb.connector.common.models.Errors.ErrorWithMessage
import com.emarsys.rdb.connector.common.models.TableSchemaDescriptors.{FieldModel, FullTableModel, TableModel}
import slick.jdbc.PostgresProfile.api._
import slick.util.AsyncExecutor

import scala.concurrent.{ExecutionContext, Future}

class RedshiftConnector (db: Database)(implicit executionContext: ExecutionContext) extends Connector {

  override def close(): Future[Unit] = {
    db.shutdown
  }

  override def testConnection(): ConnectorResponse[Unit] = {
    db.run(sql"SELECT 1".as[Int]).map(_ => Right()).recover{ case _ => Left(ErrorWithMessage("Cannot connect to the sql server")) }
  }

  override def listTables(): ConnectorResponse[Seq[TableModel]] = {
    db.run(sql"SELECT DISTINCT table_name, table_type  FROM SVV_TABLES WHERE table_schema = 'public';".as[(String, String)])
      .map(_.map(parseToTableModel))
      .map(Right(_))
      .recover {
        case ex => Left(ErrorWithMessage(ex.toString))
      }
  }

  override def listFields(tableName: String): ConnectorResponse[Seq[FieldModel]] = {
    db.run(sql"SELECT column_name, data_type FROM SVV_COLUMNS WHERE table_name = $tableName AND table_schema = 'public';".as[(String, String)])
      .map(_.map(parseToFiledModel))
      .map(Right(_))
      .recover {
        case ex => Left(ErrorWithMessage(ex.toString))
      }
  }

  override def listTablesWithFields(): ConnectorResponse[Seq[FullTableModel]] = {
    val futureMap = listAllFields()
    for {
      tablesE <- listTables()
      map <- futureMap
    } yield tablesE.map(makeTablesWithFields(_, map))
  }

  private def listAllFields(): Future[Map[String, Seq[FieldModel]]] = {
    db.run(sql"SELECT table_name, column_name, data_type FROM SVV_COLUMNS WHERE table_schema = 'public';".as[(String, String, String)])
      .map(_.groupBy(_._1).mapValues(_.map(x => parseToFiledModel(x._2 -> x._3)).toSeq))
  }

  private def makeTablesWithFields(tableList: Seq[TableModel], tableFieldMap: Map[String, Seq[FieldModel]]): Seq[FullTableModel] = {
    tableList.map(table => FullTableModel(table.name, table.isView, tableFieldMap(table.name)))
  }

  private def parseToFiledModel(f: (String, String)): FieldModel = {
    FieldModel(f._1, f._2)
  }

  private def parseToTableModel(t: (String, String)): TableModel = {
    TableModel(t._1, isTableTypeView(t._2))
  }

  private def isTableTypeView(tableType: String): Boolean = tableType match {
    case "VIEW" => true
    case _      => false
  }

}

object RedshiftConnector {
  case class RedshiftConnectionConfig(
                                       host: String,
                                       port: Int,
                                       dbName: String,
                                       dbUser: String,
                                       dbPassword: String,
                                       connectionParams: String
                                  ) extends ConnectionConfig


  def apply(config: RedshiftConnectionConfig)(executor: AsyncExecutor)(implicit executionContext: ExecutionContext): ConnectorResponse[RedshiftConnector] = {

    if (checkSsl(config.connectionParams)) {

      val db = Database.forURL(
        url = createUrl(config),
        driver = "com.amazon.redshift.jdbc42.Driver",
        user = config.dbUser,
        password = config.dbPassword,
        prop = new Properties(),
        executor = executor
      )

      Future(Right(new RedshiftConnector(db)))

    } else {
      Future(Left(ErrorWithMessage("SSL Error")))
    }
  }


  private[redshift] def checkSsl(connectionParams: String): Boolean = {
    !connectionParams.matches(".*ssl=false.*")
  }


  private[redshift] def createUrl(config: RedshiftConnectionConfig) = {
    s"jdbc:redshift://${config.host}:${config.port}/${config.dbName}${safeConnectionParams(config.connectionParams)}"
  }

  private[redshift] def safeConnectionParams(connectionParams: String) = {
    if (connectionParams.startsWith("?") || connectionParams.isEmpty) {
      connectionParams
    } else {
      s"?$connectionParams"
    }
  }
}