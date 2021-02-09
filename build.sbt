import org.scalafmt.sbt.ScalafmtPlugin.scalafmtConfigSettings

mappings in (Compile, packageDoc) := Seq() // disable javadoc generation to speedup stage
sources in (Compile, doc) := Seq.empty
publishArtifact in (Compile, packageDoc) := false

lazy val connectors: Seq[ProjectReference] = Seq(
  bigQuery,
  mssql,
  mysql,
  postgres,
  redshift,
  snowflake,
  hana
)

lazy val connectorCollection = project
  .in(file("."))
  .aggregate(common)
  .aggregate(connectorTest)
  .aggregate(connectors: _*)
  .settings(Test / fork := true)
  .settings(Global / concurrentRestrictions += Tags.limit(Tags.Test, 1))
  .settings(Dependencies.scala)
  .settings(meta: _*)
  .settings(
    publishArtifact := false,
    publish / skip := true,
    crossScalaVersions := Nil
  )

lazy val common = Project(id = "common", base = file("common"))
  .settings(
    name := s"rdb-connector-common",
    AutomaticModuleName.settings(s"com.emarsys.rdb.connector.common")
  )
  .settings(Dependencies.Common: _*)
  .settings(meta: _*)
  .settings(publishSettings: _*)

lazy val connectorTest = Project(id = "connectorTest", base = file("test"))
  .settings(
    name := s"rdb-connector-test",
    AutomaticModuleName.settings(s"com.emarsys.rdb.connector.test")
  )
  .dependsOn(common)
  .settings(Dependencies.ConnectorTest: _*)
  .settings(meta: _*)
  .settings(publishSettings: _*)

lazy val bigQuery  = connector("bigquery", Dependencies.BigQuery)
lazy val mssql     = connector("mssql", Dependencies.Mssql)
lazy val mysql     = connector("mysql", Dependencies.Mysql)
lazy val postgres  = connector("postgresql", Dependencies.Postgresql)
lazy val redshift  = connector("redshift", Dependencies.Redshift)
lazy val snowflake = connector("snowflake", Dependencies.Snowflake)
lazy val hana      = connector("hana", Dependencies.Hana).settings(doNotPublish)

lazy val ItTest = config("it") extend Test
lazy val itTestSettings = Defaults.itSettings ++ Seq(
  fork := true,
  testForkedParallel := true
) ++ scalafmtConfigSettings

def connector(projectId: String, additionalSettings: sbt.Def.SettingsDefinition*): Project =
  Project(id = projectId, base = file(projectId))
    .settings(
      name := s"rdb-connector-$projectId",
      AutomaticModuleName.settings(s"com.emarsys.rdb.connector.$projectId")
    )
    .configs(ItTest)
    .settings(inConfig(ItTest)(itTestSettings))
    .dependsOn(common, connectorTest % "test")
    .settings(Dependencies.Common: _*)
    .settings(additionalSettings: _*)
    .settings(meta: _*)
    .settings(publishSettings: _*)

lazy val meta =
  Seq(
    organization := "com.emarsys",
    licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT")),
    homepage := Some(url("https://github.com/emartech/rdb-connector-collection")),
    developers := List(
      Developer("andrasp3a", "Andras Papp", "andras.papp@emarsys.com", url("https://github.com/andrasp3a")),
      Developer("doczir", "Robert Doczi", "doczi.r@gmail.com", url("https://github.com/doczir")),
      Developer("fugafree", "Gabor Fulop", "gabor.fulop@emarsys.com", url("https://github.com/fugafree")),
      Developer("Ksisu", "Kristof Horvath", "kristof.horvath@emarsys.com", url("https://github.com/Ksisu")),
      Developer("miklos-martin", "Miklos Martin", "miklos.martin@gmail.com", url("https://github.com/miklos-martin")),
      Developer("tg44", "Gergo Torcsvari", "gergo.torcsvari@emarsys.com", url("https://github.com/tg44"))
    ),
    scmInfo := Some(
      ScmInfo(
        url("https://github.com/emartech/rdb-connector-collection"),
        "scm:git:git@github.com:emartech/rdb-connector-collection.git"
      )
    )
  )

lazy val publishSettings = Seq(
  useGpgPinentry := true,
  publishTo := Some("releases" at "https://repository.eservice.emarsys.net/repository/emartech/"),
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "repository.eservice.emarsys.net",
    sys.env.getOrElse("NEXUS_USERNAME", ""),
    sys.env.getOrElse("NEXUS_PASSWORD", "")
  ),
  publish := {
    sys.error("Skipping unsigned publishing, use publishSigned instead!")
  }
)

lazy val doNotPublish = Seq(
  publishArtifact := false,
  publishTo := Some(Resolver.file("Unused repository", file("target/nothing_to_see_here")))
)
