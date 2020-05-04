# CosmosDB

@@@ warning

**This project is not continuously tested against CosmosDB.**

@@@

[CosmosDB](https://docs.microsoft.com/en-us/azure/cosmos-db/) is a wire compatible replacement for Cassandra.

Initial testing shows that most things are working. There is one issue related to deletes.
Delete of events isn't supported because CosmosDB doesn't support range deletes yet, and has a different behavior
for clustering columns than Cassandra. This will probably be fixed in CosmosDB.

When using CosmosDB you need to configure:

```
akka.persistence.cassandra {
  compatibility.cosmosdb = on
  journal.gc-grace-seconds = 0
  events-by-tag.gc-grace-seconds = 0
}
```

The connection configuration can be defined with properties like the following example.

```
datastax-java-driver {
  # using environment variables for the values
  basic.contact-points = [${COSMOSDB_CONTACT_POINTS}]
  basic.load-balancing-policy.local-datacenter = ${COSMOSDB_DATACENTER}
  advanced.auth-provider {
    class = PlainTextAuthProvider
    username = ${COSMOSDB_USER}
    password = ${COSMOSDB_PWD}
  }
  advanced.ssl-engine-factory {
    class = DefaultSslEngineFactory
  }
  advanced.reconnect-on-init = true
}
```

You need to define the variables `COSMOSDB_CONTACT_POINTS`, `COSMOSDB_DATACENTER`, `COSMOSDB_USER`,
`COSMOSDB_PWD` as environment variables or system properties in the shell running the system.
The connection details can be found in the Microsoft Azure CosmosDB portal. 

If you don't know the datacenter you can first connect with `cqlsh` and query:
```
select * from system.local;
```

