package com.emarsys.rdb.connector.mysql.hack

import java.io.{InputStream, Reader}
import java.net.URL
import java.sql
import java.sql.{Blob, Clob, Connection, Date, NClob, ParameterMetaData, PreparedStatement, Ref, ResultSet, ResultSetMetaData, RowId, SQLType, SQLWarning, SQLXML, Statement, Time, Timestamp}
import java.util.Calendar

class RouterPreparedStatement(val preparedStatement: PreparedStatement, val query: String) extends PreparedStatement {
  private var tracker: Option[QueryExecutionTracker] = None

  override def executeQuery(): ResultSet = measure("prepareStatement.executeQuery()")(preparedStatement.executeQuery())

  override def executeUpdate(): Int = measure("prepareStatement.executeUpdate()")(preparedStatement.executeUpdate())

  override def setNull(parameterIndex: Int, sqlType: Int): Unit = preparedStatement.setNull(parameterIndex, sqlType)

  override def setBoolean(parameterIndex: Int, x: Boolean): Unit = preparedStatement.setBoolean(parameterIndex, x)

  override def setByte(parameterIndex: Int, x: Byte): Unit = preparedStatement.setByte(parameterIndex, x)

  override def setShort(parameterIndex: Int, x: Short): Unit = preparedStatement.setShort(parameterIndex, x)

  override def setInt(parameterIndex: Int, x: Int): Unit = preparedStatement.setInt(parameterIndex, x)

  override def setLong(parameterIndex: Int, x: Long): Unit = preparedStatement.setLong(parameterIndex, x)

  override def setFloat(parameterIndex: Int, x: Float): Unit = preparedStatement.setFloat(parameterIndex, x)

  override def setDouble(parameterIndex: Int, x: Double): Unit = preparedStatement.setDouble(parameterIndex, x)

  override def setBigDecimal(parameterIndex: Int, x: java.math.BigDecimal): Unit = preparedStatement.setBigDecimal(parameterIndex, x)

  override def setString(parameterIndex: Int, x: String): Unit = preparedStatement.setString(parameterIndex, x)

  override def setBytes(parameterIndex: Int, x: Array[Byte]): Unit = preparedStatement.setBytes(parameterIndex, x)

  override def setDate(parameterIndex: Int, x: Date): Unit = preparedStatement.setDate(parameterIndex, x)

  override def setTime(parameterIndex: Int, x: Time): Unit = preparedStatement.setTime(parameterIndex, x)

  override def setTimestamp(parameterIndex: Int, x: Timestamp): Unit = preparedStatement.setTimestamp(parameterIndex, x)

  override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Int): Unit = preparedStatement.setAsciiStream(parameterIndex, x, length)

  override def setUnicodeStream(parameterIndex: Int, x: InputStream, length: Int): Unit = preparedStatement.setUnicodeStream(parameterIndex, x, length)

  override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Int): Unit = preparedStatement.setBinaryStream(parameterIndex, x, length)

  override def clearParameters(): Unit = preparedStatement.clearParameters()

  override def setObject(parameterIndex: Int, x: Any, targetSqlType: Int): Unit = preparedStatement.setObject(parameterIndex, x, targetSqlType)

  override def setObject(parameterIndex: Int, x: Any): Unit = preparedStatement.setObject(parameterIndex, x)

  override def execute(): Boolean = {
    measure(s"prepareStatement.execute($query)")(preparedStatement.execute())
  }

  override def addBatch(): Unit = preparedStatement.addBatch()

  override def setCharacterStream(parameterIndex: Int, reader: Reader, length: Int): Unit = preparedStatement.setCharacterStream(parameterIndex, reader, length)

  override def setRef(parameterIndex: Int, x: Ref): Unit = preparedStatement.setRef(parameterIndex, x)

  override def setBlob(parameterIndex: Int, x: Blob): Unit = preparedStatement.setBlob(parameterIndex, x)

  override def setClob(parameterIndex: Int, x: Clob): Unit = preparedStatement.setClob(parameterIndex, x)

  override def setArray(parameterIndex: Int, x: sql.Array): Unit = preparedStatement.setArray(parameterIndex, x)

  override def getMetaData: ResultSetMetaData = preparedStatement.getMetaData

  override def setDate(parameterIndex: Int, x: Date, cal: Calendar): Unit = preparedStatement.setDate(parameterIndex, x, cal)

  override def setTime(parameterIndex: Int, x: Time, cal: Calendar): Unit = preparedStatement.setTime(parameterIndex, x, cal)

  override def setTimestamp(parameterIndex: Int, x: Timestamp, cal: Calendar): Unit = preparedStatement.setTimestamp(parameterIndex, x, cal)

  override def setNull(parameterIndex: Int, sqlType: Int, typeName: String): Unit = preparedStatement.setNull(parameterIndex, sqlType, typeName)

  override def setURL(parameterIndex: Int, x: URL): Unit = preparedStatement.setURL(parameterIndex, x)

  override def getParameterMetaData: ParameterMetaData = preparedStatement.getParameterMetaData

  override def setRowId(parameterIndex: Int, x: RowId): Unit = preparedStatement.setRowId(parameterIndex, x)

  override def setNString(parameterIndex: Int, value: String): Unit = preparedStatement.setNString(parameterIndex, value)

  override def setNCharacterStream(parameterIndex: Int, value: Reader, length: Long): Unit = preparedStatement.setNCharacterStream(parameterIndex, value, length)

  override def setNClob(parameterIndex: Int, value: NClob): Unit = preparedStatement.setNClob(parameterIndex, value)

  override def setClob(parameterIndex: Int, reader: Reader, length: Long): Unit = preparedStatement.setClob(parameterIndex, reader, length)

  override def setBlob(parameterIndex: Int, inputStream: InputStream, length: Long): Unit = preparedStatement.setBlob(parameterIndex, inputStream, length)

  override def setNClob(parameterIndex: Int, reader: Reader, length: Long): Unit = preparedStatement.setNClob(parameterIndex, reader, length)

  override def setSQLXML(parameterIndex: Int, xmlObject: SQLXML): Unit = preparedStatement.setSQLXML(parameterIndex, xmlObject)

  override def setObject(parameterIndex: Int, x: Any, targetSqlType: Int, scaleOrLength: Int): Unit = preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength)

  override def setAsciiStream(parameterIndex: Int, x: InputStream, length: Long): Unit = preparedStatement.setAsciiStream(parameterIndex, x, length)

  override def setBinaryStream(parameterIndex: Int, x: InputStream, length: Long): Unit = preparedStatement.setBinaryStream(parameterIndex, x, length)

  override def setCharacterStream(parameterIndex: Int, reader: Reader, length: Long): Unit = preparedStatement.setCharacterStream(parameterIndex, reader, length)

  override def setAsciiStream(parameterIndex: Int, x: InputStream): Unit = preparedStatement.setAsciiStream(parameterIndex, x)

  override def setBinaryStream(parameterIndex: Int, x: InputStream): Unit = preparedStatement.setBinaryStream(parameterIndex, x)

  override def setCharacterStream(parameterIndex: Int, reader: Reader): Unit = preparedStatement.setCharacterStream(parameterIndex, reader)

  override def setNCharacterStream(parameterIndex: Int, value: Reader): Unit = preparedStatement.setNCharacterStream(parameterIndex, value)

  override def setClob(parameterIndex: Int, reader: Reader): Unit = preparedStatement.setClob(parameterIndex, reader)

  override def setBlob(parameterIndex: Int, inputStream: InputStream): Unit = preparedStatement.setBlob(parameterIndex, inputStream)

  override def setNClob(parameterIndex: Int, reader: Reader): Unit = preparedStatement.setNClob(parameterIndex, reader)

  override def setObject(parameterIndex: Int, x: Any, targetSqlType: SQLType, scaleOrLength: Int): Unit = preparedStatement.setObject(parameterIndex, x, targetSqlType, scaleOrLength)

  override def setObject(parameterIndex: Int, x: Any, targetSqlType: SQLType): Unit = preparedStatement.setObject(parameterIndex, x, targetSqlType)

  override def executeLargeUpdate(): Long = measure("prepareStatement.executeLargeUpdate()")(preparedStatement.executeLargeUpdate())

  override def executeQuery(sql: String): ResultSet = measure("prepareStatement.executeQuery(sql)")(preparedStatement.executeQuery(sql))

  override def executeUpdate(sql: String): Int = measure("prepareStatement.executeUpdate(sql)")(preparedStatement.executeUpdate(sql))

  override def close(): Unit = preparedStatement.close()

  override def getMaxFieldSize: Int = preparedStatement.getMaxFieldSize

  override def setMaxFieldSize(max: Int): Unit = preparedStatement.setMaxFieldSize(max)

  override def getMaxRows: Int = preparedStatement.getMaxRows

  override def setMaxRows(max: Int): Unit = preparedStatement.setMaxRows(max)

  override def setEscapeProcessing(enable: Boolean): Unit = preparedStatement.setEscapeProcessing(enable)

  override def getQueryTimeout: Int = preparedStatement.getQueryTimeout

  override def setQueryTimeout(seconds: Int): Unit = preparedStatement.setQueryTimeout(seconds)

  override def cancel(): Unit = preparedStatement.cancel()

  override def getWarnings: SQLWarning = preparedStatement.getWarnings

  override def clearWarnings(): Unit = preparedStatement.clearWarnings()

  override def setCursorName(name: String): Unit = preparedStatement.setCursorName(name)

  override def execute(sql: String): Boolean = measure("prepareStatement.execute(sql)")(preparedStatement.execute(sql))

  override def getResultSet: ResultSet = new RouterResultSet(preparedStatement.getResultSet, tracker)

  override def getUpdateCount: Int = preparedStatement.getUpdateCount

  override def getMoreResults: Boolean = preparedStatement.getMoreResults

  override def setFetchDirection(direction: Int): Unit = preparedStatement.setFetchDirection(direction)

  override def getFetchDirection: Int = preparedStatement.getFetchDirection

  override def setFetchSize(rows: Int): Unit = preparedStatement.setFetchSize(rows)

  override def getFetchSize: Int = preparedStatement.getFetchSize

  override def getResultSetConcurrency: Int = preparedStatement.getResultSetConcurrency

  override def getResultSetType: Int = preparedStatement.getResultSetType

  override def addBatch(sql: String): Unit = preparedStatement.addBatch(sql)

  override def clearBatch(): Unit = preparedStatement.clearBatch()

  override def executeBatch(): Array[Int] = measure("prepareStatement.executeBatch()")(preparedStatement.executeBatch())

  override def getConnection: Connection = preparedStatement.getConnection

  override def getMoreResults(current: Int): Boolean = preparedStatement.getMoreResults(current)

  override def getGeneratedKeys: ResultSet = preparedStatement.getGeneratedKeys

  override def executeUpdate(sql: String, autoGeneratedKeys: Int): Int = measure("prepareStatement.executeUpdate(sql, autoGeneratedKeys)")(preparedStatement.executeUpdate(sql, autoGeneratedKeys))

  override def executeUpdate(sql: String, columnIndexes: Array[Int]): Int = measure("prepareStatement.executeUpdate(sql, columnIndexes)")(preparedStatement.executeUpdate(sql, columnIndexes))

  override def executeUpdate(sql: String, columnNames: Array[String]): Int = measure("prepareStatement.executeUpdate(sql, columnNames)")(preparedStatement.executeUpdate(sql, columnNames))

  override def execute(sql: String, autoGeneratedKeys: Int): Boolean = measure("prepareStatement.execute(sql, autoGeneratedKeys)")(preparedStatement.execute(sql, autoGeneratedKeys))

  override def execute(sql: String, columnIndexes: Array[Int]): Boolean = measure("prepareStatement.execute(sql, columnIndexes)")(preparedStatement.execute(sql, columnIndexes))

  override def execute(sql: String, columnNames: Array[String]): Boolean = measure("prepareStatement.execute(sql, columnNames)")(preparedStatement.execute(sql, columnNames))

  override def getResultSetHoldability: Int = preparedStatement.getResultSetHoldability

  override def isClosed: Boolean = preparedStatement.isClosed

  override def setPoolable(poolable: Boolean): Unit = preparedStatement.setPoolable(poolable)

  override def isPoolable: Boolean = preparedStatement.isPoolable

  override def closeOnCompletion(): Unit = preparedStatement.closeOnCompletion()

  override def isCloseOnCompletion: Boolean = preparedStatement.isCloseOnCompletion

  override def getLargeUpdateCount: Long = preparedStatement.getLargeUpdateCount

  override def setLargeMaxRows(max: Long): Unit = preparedStatement.setLargeMaxRows(max)

  override def getLargeMaxRows: Long = preparedStatement.getLargeMaxRows

  override def executeLargeBatch(): Array[Long] = measure("prepareStatement.executeLargeBatch()")(preparedStatement.executeLargeBatch())

  override def executeLargeUpdate(sql: String): Long = measure("prepareStatement.executeLargeUpdate(sql)")(preparedStatement.executeLargeUpdate(sql))

  override def executeLargeUpdate(sql: String, autoGeneratedKeys: Int): Long = measure("prepareStatement.executeLargeUpdate(sql, autoGeneratedKeys)")(preparedStatement.executeLargeUpdate(sql, autoGeneratedKeys))

  override def executeLargeUpdate(sql: String, columnIndexes: Array[Int]): Long = measure("prepareStatement.executeLargeUpdate(sql, columnIndexes)")(preparedStatement.executeLargeUpdate(sql, columnIndexes))

  override def executeLargeUpdate(sql: String, columnNames: Array[String]): Long = measure("prepareStatement.executeLargeUpdate(sql, columnNames)")(preparedStatement.executeLargeUpdate(sql, columnNames))

  override def enquoteLiteral(`val`: String): String = preparedStatement.enquoteLiteral(`val`)

  override def enquoteIdentifier(identifier: String, alwaysQuote: Boolean): String = preparedStatement.enquoteIdentifier(identifier, alwaysQuote)

  override def isSimpleIdentifier(identifier: String): Boolean = preparedStatement.isSimpleIdentifier(identifier)

  override def enquoteNCharLiteral(`val`: String): String = preparedStatement.enquoteNCharLiteral(`val`)

  override def unwrap[T](iface: Class[T]): T = preparedStatement.unwrap(iface)

  override def isWrapperFor(iface: Class[_]): Boolean = preparedStatement.isWrapperFor(iface)

  def setQueryExecutionTracker(tracker: QueryExecutionTracker): Unit = {
    if(this.tracker.isDefined) {
      println("wat" * 80)
    }
    this.tracker = Some(tracker)
  }

  private def measure[A](name: String)(f: => A): A = {
    tracker.foreach(_.started(System.nanoTime()))
    val start = System.nanoTime()
    val res = f
    tracker.foreach(_.executed(System.nanoTime()))
    val end = System.nanoTime()

    println(s"$name: ${(end - start) / 1e6f}ms")
    res
  }
}

object RouterPreparedStatement {
  def setQueryExecutionTracker(statement: Statement, tracker: QueryExecutionTracker): Statement = {
    statement match {
      case rps: RouterPreparedStatement => rps.setQueryExecutionTracker(tracker)
    }
    statement
  }
}