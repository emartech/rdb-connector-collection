name := "rdb-connector-mysql"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.4"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= {
  val scalaTestV = "3.0.1"
  Seq(
    "com.github.emartech" % "rdb-connector-common"  % "-SNAPSHOT" changing(),
    "com.typesafe.slick"  %% "slick"                % "3.2.0",
    "org.mariadb.jdbc"    %  "mariadb-java-client"  % "1.5.9",
    "org.scalatest"       %% "scalatest"            % scalaTestV  % "test"
  )
}

lazy val ItTest = config("it") extend Test

lazy val root = (project in file("."))
.configs(ItTest)
  .settings(
    inConfig(ItTest)(Seq(Defaults.itSettings: _*))
  )