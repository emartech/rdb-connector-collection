import sbt.Keys._
import sbt._

object Dependencies {

  val ScalaVersion = "2.12.6"

  val akkaVersion = "2.5.6"
  val slickVersion = "3.2.3"
  val akkaHttpVersion = "10.0.7"

  val Common = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % akkaVersion,
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion % Test,
      "org.scalatest" %% "scalatest" % "3.0.1" % Test,
      "org.mockito"         %  "mockito-core"         % "2.11.0"  % Test,
      "com.typesafe.akka"     %% "akka-http-spray-json"      % "10.0.7" % Test
    ),
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
    //scalafmtOnCompile := true,
  )

  val ConnectorTest = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream-testkit" % akkaVersion,
      "org.scalatest" %% "scalatest" % "3.0.1",
      "org.mockito"         %  "mockito-core"         % "2.11.0",
      "com.typesafe.akka"     %% "akka-http-spray-json"      % "10.0.7"
    )
  )

  val BigQuery = Seq(
    libraryDependencies ++= Seq(
    "com.typesafe.akka"   %% "akka-http-core"       % akkaHttpVersion,
    "com.typesafe.akka"   %% "akka-http-spray-json" % akkaHttpVersion,
    "com.pauldijou"       %% "jwt-core"             % "0.14.1",
    "com.github.fommil"   %% "spray-json-shapeless" % "1.3.0",
    "org.typelevel"       %% "cats-core"            % "1.0.1",
    "com.typesafe.akka" %% "akka-stream-contrib" % "0.9"
    )
  )

  val Mssql = Seq(
    libraryDependencies ++= Seq(
    "com.typesafe.slick"      %% "slick"                % slickVersion,
    "com.typesafe.slick"      %% "slick-hikaricp"       % slickVersion,
    "com.microsoft.sqlserver" %  "mssql-jdbc"           % "6.4.0.jre8"
    )
  )

  val Mysql = Seq(
    libraryDependencies ++= Seq(
    "com.typesafe.slick"  %% "slick"                 % slickVersion,
    "com.typesafe.slick"  %% "slick-hikaricp"        % slickVersion,
    "mysql"               %  "mysql-connector-java"  % "5.1.38",
    "org.typelevel"       %% "cats-core"             %  "1.0.1"
    )
  )

  val Postgresql = Seq(
    libraryDependencies ++= Seq(
    "com.typesafe.slick"  %% "slick"                % slickVersion,
    "com.typesafe.slick"  %% "slick-hikaricp"       % slickVersion,
    "org.postgresql"      %  "postgresql"           % "42.1.4"
    )
  )

  val Redshift = Seq(
    resolvers ++= Seq(
      "Amazon" at "http://redshift-maven-repository.s3-website-us-east-1.amazonaws.com/release",
    ),
    libraryDependencies ++= Seq(
      "com.typesafe.slick"  %% "slick"                % slickVersion,
      "com.typesafe.slick"  %% "slick-hikaricp"       % slickVersion,
      "com.amazon.redshift" %  "redshift-jdbc42"      % "1.2.8.1005"
    )
  )
}
