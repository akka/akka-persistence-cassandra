# Configuration

Make your edits/overrides in your `application.conf`.

## Default configuration

The reference configuration file with the default values:

@@snip [reference.conf](/core/src/main/resources/reference.conf)

Journal configuration is under `akka.persistence.cassandra.journal`.

Snapshot configuration is under `akka.persistence.cassandra.snapshot`.

Query configuration is under `akka.persistence.cassandra.query`.

Events by tag configuration is under `akka.persistence.cassandra.events-by-tag` and shared
b `journal` and `query`.

The settings that shared by the `journal`, `query`, and `snapshot` parts of the plugin and are under
`akka.persistence.cassandra`.

## Cassandra driver configuration

All Cassandra driver settings are via its @extref:[standard profile mechanism](java-driver:manual/core/configuration/).

One important setting is to configure the database driver to retry the initial connection:

`datastax-java-driver.advanced.reconnect-on-init = true`

It is not enabled automatically as it is in the driver's reference.conf and is not overridable in a profile.

### Cassandra driver overrides

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #profile }

## Contact points configuration

The Cassandra server contact points can be defined with the @extref:[Cassandra driver configuration](java-driver:manual/core/configuration/)

```
datastax-java-driver {
  basic.contact-points = ["127.0.0.1:9042"]
  basic.load-balancing-policy.local-datacenter = "datacenter1"
}
```

Alternatively, Akka Discovery can be used for finding the Cassandra server contact points as described
in the @extref:[Alpakka Cassandra documentation](alpakka:cassandra.html#using-akka-discovery).

Without any configuration it will use `localhost:9042` as default.

## Using Cassandra-as-a-Service

You can spin up a free Apache Cassandra cluster in the cloud using [DataStax
Astra](https://www.datastax.com/products/datastax-astra). To use a Cassandra-as-a-Service cluster as Akka persistence,
specify your secure connect bundle and credentials in your configuration instead of the contact points:

```
datastax-java-driver {
  basic {
    session-keyspace = my_keyspace
    cloud {
      secure-connect-bundle = /path/to/secure-connect-database_name.zip
    }
  }
  advanced {
    auth-provider {
      class = PlainTextAuthProvider
      username = <user name> 
      password = <password>
    }
  }
}
```