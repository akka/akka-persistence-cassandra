# DataStax Astra

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
