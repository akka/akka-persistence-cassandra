# End to end example

This is for testing events by tag end to end.

All events are tagged with a configurable number of tags `tag-1`, `tag-2`, etc.

Then there are N processors that each process a configured number of tags.

The write side will use processor * tags per processors as the total number of tags

There are three roles:

* write - run the persistent actors in sharding
* load - generate load to the persistent actors
* read - run the sharded daemon set to read the tagged events

The read side periodically publishes latency statistics to distributed pub sub. These are currently just logged out.

## Validation

When everything is running you should see logs on the read side with the estimated latency.
The latency is calculated from the event timestamp so if you restart without an offset the latency will look very high!


## Running locally

A cassandra cluster must be available on localhost:9042

The first node can be run with the default ports but you must provide a role and to use the local configuration e.g.:

`sbt -Dconfig.resource=local.conf  -Dakka.cluster.roles.0=write run` 

Each subsequent node needs its akka management and remoting port overriden. The second node should use port 2552 for remoting
and port 8552 as these are configured in bootstrap. 

`sbt -Dakka.remote.artery.canonical.port=2552 -Dakka.management.http.port=8552 -Dconfig.resource=local.conf -Dakka.cluster.roles.0=write run`

Then add at least one load node:

`sbt -Dakka.remote.artery.canonical.port=2553 -Dakka.management.http.port=8553 -Dconfig.resource=local.conf -Dakka.cluster.roles.0=load run`

And finally at least one read node:

`sbt -Dakka.remote.artery.canonical.port=2554 -Dakka.management.http.port=8554 -Dconfig.resource=local.conf -Dakka.cluster.roles.0=read  -Dakka.cluster.roles.1=report run`

IntelliJ run configurations are included.

## Running inside a Kubernetes Cluster

Configuration and sbt-native-packager are included for running in K8s for larger scale tests.

There are three deployments in the `kubernetes` folder. One for each role. They all join the same cluster
and use the same image. They have imagePullPolicy set to Never for minikube, remove this for a real cluster.


### Running in minikube

* Create the Cassandra cluster, this also includes a service.

`kubectl apply -f kubernetes/cassandra.yaml`

* Create the write side: 

`kubectl apply -f kubernetes/write.yaml`

* Generate some load:

`kubectl apply -f kubernetes/load.yaml`

* Start the event processors, these also include logging out the aggregated latency every 10s

`kubectl apply -f kubernetes/read.yaml`

If everything is working the read side should get logs like this:

```
Read side Count: 1 Max: 100 p99: 100 p50: 100
Read side Count: 1 Max: 97 p99: 97 p50: 97
Read side Count: 3 Max: 101 p99: 101 p50: 100
```

To increase the load edit values in `common.conf` e.g. increase the load-tick duration.


### Running in a real Kubernetes cluster

#### Publish to a registry the cluster can access e.g. Dockerhub with the kubakka user

The app image must be in a registry the cluster can see.
The build.sbt uses DockerHub and the `kubakka` user. Update this if your cluster can't 
access DockerHub.

To push an image to docker hub run:

`sbt docker:publish`

And remove the imagePullPolicy: Never from the deployments

#### Configure Cassandra 

Update `kubernetes.conf` to point to a Cassandra cluster e.g. via a Kubernetes service.
