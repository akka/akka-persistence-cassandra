# End to end example

This is for testing events by tag end to end.

All events are tagged with a configurable number of tags `tag-1`, `tag-2`, etc.

Then there are N processors that each process a configured number of tags.

The write side will use processor * tags per processors as the toal number of tags

There are three roles:

* write - run the persistent actors in sharding
* load - generate load to the persistent actors
* read - run the sharded daemon set to read the tagged events

The read side periodically publishes latency statistics to distributed pub sub. These are currently just logged out.

## Running locally

A cassandra cluster must be available on localhost:9042

The first node can run with the default ports for remoting and akka management. The config location
and the role must be specified as JVM options:

`-Dconfig.resource=local.conf  -Dakka.cluster.roles.0=write`

Each node needs its akka management and remoting port overriden. For the cluster to join the default settings with `local.conf`
and ports 2552 and 8552 need to be running.

`-Dakka.remote.artery.canonical.port=2552 -Dakka.management.http.port=8552 -Dconfig.resource=local.conf -Dakka.cluster.roles.0=write`

At least one load node:

`-Dakka.remote.artery.canonical.port=2553 -Dakka.management.http.port=8553 -Dconfig.resource=local.conf -Dakka.cluster.roles.0=load`

And finally at least one read node:

`-Dakka.remote.artery.canonical.port=2554 -Dakka.management.http.port=8554 -Dconfig.resource=local.conf -Dakka.cluster.roles.0=read`

IntelliJ run configurations are included.

## Running inside a Kubernetes Cluster

Configuration and sbt-native-packager are included for running in K8s.

There are three deployments in the `kubernetes` folder. One for each role. They all join the same cluster
and use the same image. They have imagePullPolicy set to Never for minikube, remove this for a real cluster.

### Configure your docker registry and username

The app image must be in a registry the cluster can see.
The build.sbt uses dockerhub and the `kubakka` user. Update this if your cluster can't 
access dockerhub.

To push an image to docker hub run:

`sbt docker:publlish`

### Configure Cassnadra 

Update `kubernetes.conf` to point to a Cassandra cluster e.g. via a Kubernetes service.




