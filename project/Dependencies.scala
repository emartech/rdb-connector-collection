import sbt.Keys._
import sbt._

object Dependencies {

  val ScalaVersion = "2.12.10"

  val mssqlVersion    = "6.4.0.jre8"
  val mysqlVersion    = "8.0.18"
  val postgresVersion = "42.2.9"
  val redshiftVersion = "1.2.37.1061"

  val akkaVersion               = "2.5.27"
  val akkaHttpVersion           = "10.1.11"
  val akkaStreamContribVersion  = "0.10"
  val slickVersion              = "3.3.2"
  val catsCoreVersion           = "2.1.0"
  val jwtVersion                = "4.1.0"
  val enumeratumVersion         = "1.5.15"
  val sprayJsonShapelessVersion = "1.4.0"

  val scalatestVersion    = "3.0.8"
  val mockitoScalaVersion = "1.10.2"
  val mockitoVersion      = "3.2.4"

  val scala = Seq(
    scalaVersion := ScalaVersion,
    scalacOptions ++= Seq(
      "-encoding",
      "UTF-8",
      "-feature",
      "-language:_",
      "-unchecked",
      "-deprecation",
      //"-Xfatal-warnings",
      "-Xlint",
      "-Yno-adapted-args",
      "-Ywarn-dead-code",
      "-Ypartial-unification",
      "-Xfuture",
      "-target:jvm-1.8"
    )
  )

  val Common = scala ++ Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream"          % akkaVersion,
      "org.typelevel"     %% "cats-core"            % catsCoreVersion,
      "com.typesafe.akka" %% "akka-stream-testkit"  % akkaVersion % Test,
      "org.scalatest"     %% "scalatest"            % scalatestVersion % Test,
      "org.mockito"       %% "mockito-scala"        % mockitoScalaVersion % Test,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion % Test,
      "com.beachape"      %% "enumeratum"           % enumeratumVersion
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
      "com.github.fommil" %% "spray-json-shapeless" % sprayJsonShapelessVersion,
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
    )
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
}
