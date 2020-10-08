# Amazon Keyspaces

@@@ warning

**This project is not continuously tested against Keyspaces, and Keyspaces is not supported as part of the [Lightbend Subscription](https://www.lightbend.com/lightbend-subscription).**

@@@

[Amazon Keyspaces](https://docs.aws.amazon.com/keyspaces) is an Amazon service
that provides a Cassandra-like API, but it's not real Cassandra and has
different behaviour for some things (for example atomic persist of several
events is not possible).

Keyspaces is known not to work on akka-persistence-cassandra version 1.0.3 and
earlier.

## Configuration

Configure the authentication plugin in application.conf:

```
datastax-java-driver { 
  basic.contact-points = [ "cassandra.eu-central-1.amazonaws.com:9142"]
  basic.request.consistency = LOCAL_QUORUM
  basic.load-balancing-policy {
    class = DefaultLoadBalancingPolicy
    local-datacenter = eu-central-1
  }
  advanced {
    auth-provider = {
      class = software.aws.mcs.auth.SigV4AuthProvider
      aws-region = eu-central-1
    }
    ssl-engine-factory {
      class = DefaultSslEngineFactory
    }
  }
}
```

Note that the contact points must also be configured, which isn’t fully documented. You find the endpoints for different regions at https://docs.aws.amazon.com/keyspaces/latest/devguide/programmatic.endpoints.html

## Notes

Keyspace and table creation are asynchronous and not finished when the
returned Future is completed, so it is best to create the tables and keyspaces
manually before deploying your application.
