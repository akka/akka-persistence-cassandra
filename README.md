Akka
====
*Akka is a powerful platform that simplifies building and operating highly responsive, resilient, and scalable services.*


The platform consists of
* the [**Akka SDK**](https://doc.akka.io/java/index.html) for straightforward, rapid development with AI assist and automatic clustering. Services built with the Akka SDK are automatically clustered and can be deployed on any infrastructure.
* and [**Akka Automated Operations**](https://doc.akka.io/operations/akka-platform.html), a managed solution that handles everything for Akka SDK services from auto-elasticity to multi-region high availability running safely within your VPC.

The **Akka SDK** and **Akka Automated Operations** are built upon the foundational [**Akka libraries**](https://doc.akka.io/libraries/akka-dependencies/current/), providing the building blocks for distributed systems.


Cassandra Plugins for Akka Persistence
======================================

Replicated [Akka Persistence](https://doc.akka.io/libraries/akka-core/current/scala/persistence.html) journal and snapshot store backed by [Apache Cassandra](https://cassandra.apache.org/).

For questions please use the [discuss.akka.io](https://discuss.akka.io/c/akka/).

Reference Documentation
-----------------------

The reference documentation for all Akka libraries is available via [doc.akka.io/libraries/](https://doc.akka.io/libraries/), details for the Akka Cassandra Plugin
for [Scala](https://doc.akka.io/libraries/akka-persistence-cassandra/current/?language=scala) and [Java](https://doc.akka.io/libraries/akka-persistence-cassandra/current/?language=java).

The current versions of all Akka libraries are listed on the [Akka Dependencies](https://doc.akka.io/libraries/akka-dependencies/current/) page. Releases of the Akka Cassandra Plugin in this repository are listed on the [GitHub releases](https://github.com/akka/akka-persistence-cassandra/releases) page.



## History

This [Apache Cassandra](https://cassandra.apache.org/) plugin to Akka Persistence was initiated [originally](https://github.com/krasserm/akka-persistence-cassandra) by Martin Krasser, [@krasserm](https://github.com/krasserm) in 2014.

It moved to the [Akka](https://github.com/akka/) organisation in 2016 and the first release after that move was 0.7 in January 2016.

## License

Akka is licensed under the Business Source License 1.1, please see the [Akka License FAQ](https://www.lightbend.com/akka/license-faq).

Tests and documentation are under a separate license, see the LICENSE file in each documentation and test root directory for details.
