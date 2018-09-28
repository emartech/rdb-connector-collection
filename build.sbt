
lazy val connectors: Seq[ProjectReference] = Seq(
  bigQuery,
  mssql,
  mysql,
  postgres,
  redshift
)

lazy val connectorCollection = project
  .in(file("."))
  .aggregate(common)
  .aggregate(connectorTest)
  .aggregate(connectors: _*)

lazy val common = Project(id = "common", base = file("common"))
  .settings(
    name := s"rdb-connector-common",
    AutomaticModuleName.settings(s"com.emarsys.rdb.connector.common")
  )
  //.settings(exports(Seq("com.emarsys.rdb.connector.common.*")))
  .settings(Dependencies.Common: _*)
lazy val connectorTest = Project(id = "connectorTest", base = file("test"))
  .settings(
    name := s"rdb-connector-test",
    AutomaticModuleName.settings(s"com.emarsys.rdb.connector.test")
  )
  .dependsOn(common)
  .settings(Dependencies.ConnectorTest: _*)
  //.settings(exports(Seq("com.emarsys.rdb.connector.test.*")))

lazy val bigQuery = connector("bigquery", Dependencies.BigQuery)
lazy val mssql = connector("mssql", Dependencies.Mssql)
lazy val mysql = connector("mysql", Dependencies.Mysql)
lazy val postgres = connector("postgresql", Dependencies.Postgresql)
lazy val redshift = connector("redshift", Dependencies.Redshift)

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
