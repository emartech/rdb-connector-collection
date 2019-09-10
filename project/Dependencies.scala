import sbt.Keys._
import sbt._

object Dependencies {

  val ScalaVersion = "2.12.8"

  val akkaVersion     = "2.5.6"
  val slickVersion    = "3.3.0"
  val akkaHttpVersion = "10.0.7"
  val catsCoreVersion = "2.0.0"

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
      "org.scalatest"     %% "scalatest"            % "3.0.1" % Test,
      "org.mockito"       % "mockito-core"          % "2.28.2" % Test,
      "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.7" % Test,
      "com.beachape"      %% "enumeratum"           % "1.5.13"
    )
  )

  val ConnectorTest = scala ++ Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream-testkit"  % akkaVersion,
      "org.scalatest"     %% "scalatest"            % "3.0.1",
      "org.mockito"       % "mockito-core"          % "2.28.2",
      "com.typesafe.akka" %% "akka-http-spray-json" % "10.0.7"
    )
  )

  val BigQuery = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http-core"       % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.pauldijou"     %% "jwt-core"             % "0.14.1",
      "com.github.fommil" %% "spray-json-shapeless" % "1.3.0",
      "org.typelevel"     %% "cats-core"            % catsCoreVersion,
      "com.typesafe.akka" %% "akka-stream-contrib"  % "0.9"
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
    resolvers ++= Seq(
      ("Amazon" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release").withAllowInsecureProtocol(true)
    ),
    libraryDependencies ++= Seq(
      "com.typesafe.slick"  %% "slick"          % slickVersion,
      "com.typesafe.slick"  %% "slick-hikaricp" % slickVersion,
      "com.amazon.redshift" % "redshift-jdbc42" % "1.2.8.1005",
      "org.typelevel"       %% "cats-core"      % catsCoreVersion
    )
  )
}
