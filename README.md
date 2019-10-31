# Rdb - connector - collection

[ ![Codeship Status for emartech/rdb-connector-collection](https://app.codeship.com/projects/bda87020-b021-0136-2e3a-02cacefff18b/status?branch=master)](https://app.codeship.com/projects/310361)

## Tasks:

### common

Defines a Connector trait, that every connector should implement. Contains the common logic, case classes and some default implementations, that may be overwritten by specific connectors and may use functions implemented in the connectors. (eg. validation)

### test

Contains general test implementations, that may be used in connectors to test connector specific behavior. When applied on a given connector, all the test assertions should be fulfilled assuming correct implementation.

### bigquery/mssql/mysql/postgresql/redshift

Implements the general database connector trait, and contains bigquery/mssql/mysql/postgresql/redshift
 specific implementation.

## Creating a release

Choose the appropriate version number, then create and push a git tag, prefixed with `v`.
To create an unsigned, ful tag object:
```
$ git tag -s v1.0.3
$ git push --tag
```
To create a signed, ful tag object:
```
$ git tag -a v1.0.3
$ git push --tag
```
After pushing the tag, while it is not strictly necessary, please [draft a release on github] with this tag too.

[sbt-dynver]: https://github.com/dwijnand/sbt-dynver
[semver]: https://semver.org
[draft a release on github]: https://github.com/emartech/db-router-client/releases/new
