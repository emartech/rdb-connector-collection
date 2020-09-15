import sbt._
import sbt.Keys._

object Dependencies {

  val v2_12                  = "2.12.11"
  val v2_13                  = "2.13.2"
  val supportedScalaVersions = Seq(v2_12, v2_13)

  val mssqlVersion     = "6.4.0.jre8"
  val mysqlVersion     = "8.0.18"
  val postgresVersion  = "42.2.14"
  val redshiftVersion  = "1.2.37.1061"
  val snowflakeVersion = "3.12.12"

  val akkaVersion              = "2.5.27"
  val akkaHttpVersion          = "10.1.11"
  val akkaStreamContribVersion = "0.10"
  val slickVersion             = "3.3.2"
  val catsCoreVersion          = "2.0.0"
  val jwtVersion               = "4.1.0"
  val enumeratumVersion        = "1.5.15"

  val scalatestVersion    = "3.0.8"
  val mockitoScalaVersion = "1.10.2"
  val mockitoVersion      = "3.2.4"

  private val defaultScalacOptions = Seq(
    "-encoding",
    "UTF-8",
    "-feature",
    "-language:_",
    "-unchecked",
    "-deprecation",
    "-Xlint",
    "-Ywarn-dead-code",
    "-target:jvm-1.8"
  )

  def scalacOptionsVersionAware(version: String) = CrossVersion.partialVersion(version) match {
    case Some((2, 12)) => defaultScalacOptions ++ Seq("-Ypartial-unification", "-Xfuture", "-Yno-adapted-args")
    case _             => defaultScalacOptions
  }

  val scala = Seq(
    crossScalaVersions := supportedScalaVersions,
    scalacOptions ++= scalacOptionsVersionAware(scalaVersion.value)
  )

  val Common = scala ++ Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka"      %% "akka-stream"             % akkaVersion,
      "org.typelevel"          %% "cats-core"               % catsCoreVersion,
      "com.beachape"           %% "enumeratum"              % enumeratumVersion,
      "com.typesafe.akka"      %% "akka-stream-testkit"     % akkaVersion % Test,
      "org.scalatest"          %% "scalatest"               % scalatestVersion % Test,
      "org.mockito"            %% "mockito-scala"           % mockitoScalaVersion % Test,
      "com.typesafe.akka"      %% "akka-http-spray-json"    % akkaHttpVersion % Test,
      "com.typesafe.akka"      %% "akka-stream-testkit"     % akkaVersion % Test,
      "org.scala-lang.modules" %% "scala-collection-compat" % "2.1.4"
    )
  )

  val ConnectorTest = scala ++ Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream-testkit"  % akkaVersion,
      "org.scalatest"     %% "scalatest"            % scalatestVersion,
      "org.mockito"       % "mockito-core"          % mockitoVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion
    )
  )

  val BigQuery = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http-core"       % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.pauldijou"     %% "jwt-core"             % jwtVersion,
      "org.typelevel"     %% "cats-core"            % catsCoreVersion,
      "com.typesafe.akka" %% "akka-stream-contrib"  % akkaStreamContribVersion
    )
  )

  val Mssql = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.slick"      %% "slick"          % slickVersion,
      "com.typesafe.slick"      %% "slick-hikaricp" % slickVersion,
      "com.microsoft.sqlserver" % "mssql-jdbc"      % mssqlVersion,
      "org.typelevel"           %% "cats-core"      % catsCoreVersion
    ),
    // disable parallel test run to prevent random deadlocks
    Test / parallelExecution := false
  )

  val Mysql = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.slick" %% "slick"               % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp"      % slickVersion,
      "mysql"              % "mysql-connector-java" % mysqlVersion,
      "org.typelevel"      %% "cats-core"           % catsCoreVersion
    )
  )

  val Postgresql = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.slick" %% "slick"          % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp" % slickVersion,
      "org.postgresql"     % "postgresql"      % postgresVersion,
      "org.typelevel"      %% "cats-core"      % catsCoreVersion
    )
  )

  val Redshift = Seq(
    resolvers ++= Seq(("Amazon" at "https://s3.amazonaws.com/redshift-maven-repository/release")),
    libraryDependencies ++= Seq(
      "com.typesafe.slick"  %% "slick"          % slickVersion,
      "com.typesafe.slick"  %% "slick-hikaricp" % slickVersion,
      "com.amazon.redshift" % "redshift-jdbc42" % redshiftVersion,
      "org.typelevel"       %% "cats-core"      % catsCoreVersion
    )
  )

  val Snowflake = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.slick" %% "slick"          % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp" % slickVersion,
      "net.snowflake"      % "snowflake-jdbc"  % snowflakeVersion,
      "org.typelevel"      %% "cats-core"      % catsCoreVersion
    )
  )
}
