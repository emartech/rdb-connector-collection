
lazy val connectors: Seq[ProjectReference] = Seq(
//  bigQuery,
//  mssql,
  mysql,
//  postgres,
//  redshift
)

lazy val connectorCollection = project
  .in(file("."))
  .aggregate(common)
  .aggregate(connectorTest)
  .aggregate(connectors: _*)
  .settings(Test / fork := true)
  .settings(Global / concurrentRestrictions += Tags.limit(Tags.Test, 1))
  .settings(meta: _*)

lazy val common = Project(id = "common", base = file("common"))
  .settings(
    name := s"rdb-connector-common",
    AutomaticModuleName.settings(s"com.emarsys.rdb.connector.common")
  )
  //.settings(exports(Seq("com.emarsys.rdb.connector.common.*")))
  .settings(Dependencies.Common: _*)
  .settings(meta: _*)
lazy val connectorTest = Project(id = "connectorTest", base = file("test"))
  .settings(
    name := s"rdb-connector-test",
    AutomaticModuleName.settings(s"com.emarsys.rdb.connector.test")
  )
  .dependsOn(common)
  .settings(Dependencies.ConnectorTest: _*)
  .settings(meta: _*)
  //.settings(exports(Seq("com.emarsys.rdb.connector.test.*")))

//lazy val bigQuery = connector("bigquery", Dependencies.BigQuery)
//lazy val mssql = connector("mssql", Dependencies.Mssql)
lazy val mysql = connector("mysql", Dependencies.Mysql)
//lazy val postgres = connector("postgresql", Dependencies.Postgresql)
//lazy val redshift = connector("redshift", Dependencies.Redshift)

lazy val ItTest = config("it") extend Test

def connector(projectId: String, additionalSettings: sbt.Def.SettingsDefinition*): Project =
  Project(id = projectId, base = file(projectId))
    .settings(
      name := s"rdb-connector-$projectId",
      AutomaticModuleName.settings(s"com.emarsys.rdb.connector.$projectId")
    )
    .configs(ItTest)
    .settings(
      inConfig(ItTest)(Seq(Defaults.itSettings: _*))
    )
    .dependsOn(common, connectorTest % "test")
    .settings(Dependencies.Common: _*)
    .settings(additionalSettings: _*)
    .settings(meta: _*)

lazy val meta =
  Seq(
    organization := "com.emarsys",
    sonatypeProfileName := "com.emarsys",
    licenses := Seq("MIT" -> url("http://opensource.org/licenses/MIT")),
    homepage := Some(url("https://github.com/emartech/rdb-connector-collection")),
    developers := List(
      Developer("andrasp3a", "Andras Papp", "andras.papp@emarsys.com", url("https://github.com/andrasp3a")),
      Developer("doczir", "Robert Doczi", "doczi.r@gmail.com", url("https://github.com/doczir")),
      Developer("fugafree", "Gabor Fulop", "gabor.fulop@emarsys.com", url("https://github.com/fugafree")),
      Developer("Ksisu", "Kristof Horvath", "kristof.horvath@emarsys.com", url("https://github.com/Ksisu")),
      Developer("miklos-martin", "Miklos Martin", "miklos.martin@gmail.com", url("https://github.com/miklos-martin")),
      Developer("tg44", "Gergo Torcsvari", "gergo.torcsvari@emarsys.com", url("https://github.com/tg44")),
    ),
    scmInfo := Some(ScmInfo(url("https://github.com/emartech/rdb-connector-collection"), "scm:git:git@github.com:emartech/rdb-connector-collection.git")),

    // These are the sbt-release-early settings to configure
    pgpPublicRing := file("./ci/pubring.asc"),
    pgpSecretRing := file("./ci/secring.asc"),
    releaseEarlyWith := SonatypePublisher
  )

