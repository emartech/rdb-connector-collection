import org.scalafmt.sbt.ScalafmtPlugin.scalafmtConfigSettings

lazy val commonSettings = Seq(
  scalaVersion := "2.12.3",
  organization := "com.emarsys",
  scalafmtOnCompile := true
)

lazy val ItTest = config("it") extend Test

lazy val itTestSettings = Defaults.itSettings ++ scalafmtConfigSettings ++ Seq(
  parallelExecution := false
)

lazy val root = (project in file("."))
  .configs(ItTest)
  .settings(inConfig(ItTest)(itTestSettings): _*)
  .settings(commonSettings: _*)
  .settings(
    name := "rdb-connector-bigquery",
    version := "0.1",
    resolvers += "jitpack" at "https://jitpack.io",
    scalacOptions ++= Seq(
      "-deprecation",
      "-encoding", "UTF-8",
      "-unchecked",
      "-feature",
      "-language:_",
      "-Ypartial-unification",
      "-Ywarn-dead-code",
      "-Xlint"
    ),
    libraryDependencies ++= {
      val scalaTestV = "3.0.1"
      val akkaHttpV  = "10.0.7"
      Seq(
        "com.github.emartech" % "rdb-connector-common"  % "f77f482513",
        "com.typesafe.akka"   %% "akka-http-core"       % akkaHttpV,
        "com.typesafe.akka"   %% "akka-http-spray-json" % akkaHttpV,
        "com.pauldijou"       %% "jwt-core"             % "0.14.1",
        "org.scalatest"       %% "scalatest"            % scalaTestV % Test,
        "com.typesafe.akka"   %% "akka-stream-testkit"  % "2.5.6" % Test,
        "com.github.emartech" % "rdb-connector-test"    % "03b76a4b35" % Test,
        "com.github.fommil"   %% "spray-json-shapeless" % "1.3.0",
        "org.typelevel"       %% "cats-core"            % "1.0.1",
        "com.typesafe.akka"   %% "akka-http-spray-json" % "10.0.7" % Test,
        "org.mockito"         % "mockito-core"          % "2.11.0" % Test,
        "com.typesafe.akka"   %% "akka-stream-contrib"  % "0.9"
      )
    }
  )

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt it:scalafmt")
addCommandAlias("testAll", "; test ; it:test")
