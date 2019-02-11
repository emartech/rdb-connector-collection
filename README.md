# Rdb - connector - collection

[ ![Codeship Status for emartech/rdb-connector-collection](https://app.codeship.com/projects/bda87020-b021-0136-2e3a-02cacefff18b/status?branch=master)](https://app.codeship.com/projects/310361) [![Maven Central](https://img.shields.io/maven-central/v/com.emarsys/rdb-connector-redshift_2.12.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22com.emarsys%22%20AND%20a:%22rdb-connector-redshift_2.12%22)

## Definitions:

**Router** - instantiates a specific type of connector
 
**Database connector** - implements an interface, so that the router can be connected to the specific type of database.

## Tasks:

### common

Defines a Connector trait, that every connector should implement. Contains the common logic, case classes and some default implementations, that may be overwritten by specific connectors, and may use functions implemented in the connectors. (eg. validation)

### test

Contains general test implementations, that may be used in connectors to test connector specific behavior. When applied on a given connector, all the test assertions should be fulfilled assuming correct implementation.

### bigquery/mssql/mysql/postgresql/redshift

Implements the general database connector trait, and contains bigquery/mssql/mysql/postgresql/redshift
 specific implementation.

