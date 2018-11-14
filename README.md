# Rdb - connector - collection

## Definitions:

**Router** - instantiates a specific type of connector
 
**Database connector** - implements an interface, so that the router can be connected to the specific type of database.

##Tasks:

###common

Defines a Connector trait, that every connector should implement. Contains the common logic, case classes and some default implementations, that may be overwritten by specific connectors, and may use functions implemented in the connectors. (eg. validation)

### test

Contains general test implementations, that may be used in connectors to test connector specific behavior. When applied on a given connector, all the test assertions should be fulfilled assuming correct implementation.

### bigquery/mssql/mysql/postgresql/redshift

Implements the general database connector trait, and contains bigquery/mssql/mysql/postgresql/redshift
 specific implementation.
