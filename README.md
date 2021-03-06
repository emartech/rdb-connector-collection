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

Choose the appropriate version number (you can list the tags with `git tag -l`), then create and push a git tag, prefixed with `v`.
To create a signed, ful tag object:
```
$ git tag -s v1.2.0
$ git push --tag
```
To create an unsigned, ful tag object:
```
$ git tag -a v1.2.0
$ git push --tag
```
After pushing the tag, while it is not strictly necessary, please [draft a release on github] with this tag too.

[sbt-dynver]: https://github.com/dwijnand/sbt-dynver
[semver]: https://semver.org
[draft a release on github]: https://github.com/emartech/db-router-client/releases/new

## Test databases

Two of the databases used for IT tests are hosted on external services:  
- Redshift on AWS  
- BigQuery on GCP  

### Redshift

Hosted on AWS. Sign in via http://sso.emarsys.com/.  
Region: `eu-central-1`  
Cluster: `rdb-router-test`

It is paused at nights automatically. If you would like to use it when it stopped by default, feel free to start it anytime.
