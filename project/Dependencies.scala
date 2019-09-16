import sbt.Keys._
import sbt._

object Dependencies {

  val ScalaVersion = "2.12.8"

  val akkaVersion      = "2.5.25"
  val slickVersion     = "3.3.2"
  val akkaHttpVersion  = "10.1.9"
  val catsCoreVersion  = "2.0.0"
  val scalatestVersion = "3.0.8"

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
      "org.mockito"       %% "mockito-scala"        % "1.5.16" % Test,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion % Test,
      "com.beachape"      %% "enumeratum"           % "1.5.13"
    )
  )

  val ConnectorTest = scala ++ Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream-testkit"  % akkaVersion,
      "org.scalatest"     %% "scalatest"            % scalatestVersion,
      "org.mockito"       % "mockito-core"          % "2.28.2",
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion
    )
  )

  val BigQuery = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http-core"       % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.pauldijou"     %% "jwt-core"             % "0.14.1",
      "com.github.fommil" %% "spray-json-shapeless" % "1.4.0",
      "org.typelevel"     %% "cats-core"            % catsCoreVersion,
      "com.typesafe.akka" %% "akka-stream-contrib"  % "0.10"
    )
  )

  val Mssql = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.slick"      %% "slick"          % slickVersion,
      "com.typesafe.slick"      %% "slick-hikaricp" % slickVersion,
      "com.microsoft.sqlserver" % "mssql-jdbc"      % "6.4.0.jre8",
      "org.typelevel"           %% "cats-core"      % catsCoreVersion
    )
  )

  val Mysql = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.slick" %% "slick"               % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp"      % slickVersion,
      "mysql"              % "mysql-connector-java" % "8.0.16",
      "org.typelevel"      %% "cats-core"           % catsCoreVersion
    )
  )

  val Postgresql = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.slick" %% "slick"          % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp" % slickVersion,
      "org.postgresql"     % "postgresql"      % "42.1.4",
      "org.typelevel"      %% "cats-core"      % catsCoreVersion
    )
  )

  val Redshift = Seq(
    resolvers ++= Seq(("Amazon" at "https://s3.amazonaws.com/redshift-maven-repository/release")),
    libraryDependencies ++= Seq(
      "com.typesafe.slick"  %% "slick"          % slickVersion,
      "com.typesafe.slick"  %% "slick-hikaricp" % slickVersion,
      "com.amazon.redshift" % "redshift-jdbc42" % "1.2.8.1005",
      "org.typelevel"       %% "cats-core"      % catsCoreVersion
    )
  )
}
