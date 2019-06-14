# ScyllaDB

@@@ warning

**This project is not continuously tested against ScyllaDB.**

@@@

Some of tests are run on CircleCI in External mode only.

[![CircleCI](https://circleci.com/gh/akka/akka-persistence-cassandra.svg?style=svg)](https://circleci.com/gh/btomala/akka-persistence-cassandra)

ScyllaDB is a wire compatible replacement for Cassandra. You can run the tests
by setting `akka.persistence.cassandra.CassandraLifecycle.mode = External` and having a ScyllaDB instance accessible on localhost port 9042.
Initial testing shows that there are issues when using Scylla with persistent query which is also used for recovery. Users
with ScyllaDB experience and test environments are very welcome contributors!

For the tests and for versions before 0.80 you just run ScyllaDB with `experimental: true` to use materialized views. Versions 0.80 and onward
do not use materialized views but there are tests that do for testing the migration.

The following tests don't run in external mode as they require more control over the database:
- ReconnectSpec
- SSL tests 

