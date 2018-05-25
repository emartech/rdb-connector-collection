name := "rdb-connector-bigquery"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.3"

scalacOptions := Seq(
  "-feature",
  "-deprecation",
  "-language:higherKinds",
  "-Ypartial-unification"
)

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= {
  val scalaTestV = "3.0.1"
  val akkaHttpV  = "10.0.7"
  Seq(
    "com.github.emartech"   %  "rdb-connector-common"  % "e652d3f091",
    "com.typesafe.akka"     %% "akka-http-core"        % akkaHttpV,
    "com.typesafe.akka"     %% "akka-http-spray-json"  % akkaHttpV,
    "io.igl"                %% "jwt"                   % "1.2.2",
    "org.scalatest"         %% "scalatest"             % scalaTestV   % Test,
    "com.typesafe.akka"     %% "akka-stream-testkit"   % "2.5.6"      % Test,
    "com.github.emartech"   %  "rdb-connector-test"    % "c7b8b3ae04" % Test,
    "com.github.fommil"     %% "spray-json-shapeless"  % "1.3.0",
    "org.typelevel"         %% "cats-core"             % "1.0.1",
    "com.typesafe.akka"     %% "akka-http-spray-json"  % "10.0.7"     % Test,
    "org.mockito"           %  "mockito-core"          % "2.11.0"     % Test,
    "com.typesafe.akka"     %% "akka-stream-contrib"   % "0.9"
  )
}

lazy val ItTest = config("it") extend Test

lazy val root = (project in file("."))
.configs(ItTest)
  .settings(
    inConfig(ItTest)(Seq(Defaults.itSettings: _*))
  )
