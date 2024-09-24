# Health check

A [health check for Akka Management](https://doc.akka.io/libraries/akka-management/current/healthchecks.html)
is provided. To enable it you need to add the following configuration

```
akka.management {
  health-checks {
    readiness-checks {
      akka-persistence-cassandra = "akka.persistence.cassandra.healthcheck.CassandraHealthCheck"
    }
  }
}
```

By default it will try to query the `system.local` table. The query can be configured with:

```
akka.persistence.cassandra.healthcheck {
  health-check-cql = "SELECT now() FROM system.local"
}
``` 

 
