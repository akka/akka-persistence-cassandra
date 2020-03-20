# ScyllaDB

@@@ warning

**This project is not continuously tested against ScyllaDB.**

@@@

[ScyllaDB](https://www.scylladb.com) is a wire compatible replacement for Cassandra. Initial testing shows that
there are issues when using ScyllaDB with persistent query which is also used for recovery.

Users with ScyllaDB experience and test environments are very welcome contributors!
You can run the tests and having a ScyllaDB instance accessible on localhost port 9042, or adjust the
`datastax-java-driver.basic.contact-points` and other configuration to connect to your ScyllaDB.

