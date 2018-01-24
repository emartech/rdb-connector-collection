name := "rdb-connector-bigquery"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.3"

scalacOptions := Seq(
  "-feature",
  "-deprecation",
  "-language:higherKinds"
)

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= {
  val scalaTestV = "3.0.1"
  val akkaHttpV  = "10.0.7"
  Seq(
    "com.github.emartech"   %  "rdb-connector-common"  % "-SNAPSHOT" changing(),
    "com.typesafe.akka"     %% "akka-http-core"        % akkaHttpV,
    "com.typesafe.akka"     %% "akka-http-spray-json"  % akkaHttpV,
    "io.igl"                %% "jwt"                   % "1.2.2",
    "org.scalatest"         %% "scalatest"             % scalaTestV  % "test",
    "com.typesafe.akka"     %% "akka-stream-testkit"   % "2.5.6"     % "test",
    "com.github.emartech"   %  "rdb-connector-test"    % "-SNAPSHOT" % "test" changing(),
    "com.github.fommil"     %% "spray-json-shapeless"  % "1.3.0",
    "org.typelevel"         %% "cats-core"             % "1.0.1"

  )
}

lazy val ItTest = config("it") extend Test

lazy val root = (project in file("."))
.configs(ItTest)
  .settings(
    inConfig(ItTest)(Seq(Defaults.itSettings: _*))
  )
