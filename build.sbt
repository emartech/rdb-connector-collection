
name := "rdb-connector-test"

version := "0.1-SNAPSHOT"

scalaVersion := "2.12.3"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= {
  val scalaTestV = "3.0.1"
  Seq(
    "com.github.emartech" % "rdb-connector-common"  % "-SNAPSHOT" changing(),
    "org.scalatest"       %% "scalatest"            % scalaTestV,
    "org.mockito"         %  "mockito-core"         % "2.11.0"  % Test
  )
}