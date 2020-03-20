# Query Plugin

It implements the following @extref:[Persistence Queries](akka:persistence-query.html):

* eventsByPersistenceId, currentEventsByPersistenceId
* eventsByTag, currentEventsByTag
* persistenceIds, currentPersistenceIds 

See API details in @apidoc[CassandraReadJournal] and @ref:[eventsByTag documentation](events-by-tag.md). 

Persistence Query usage example to obtain a stream with all events tagged with "someTag" with Persistence Query:

    val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
    queries.eventsByTag("someTag", Offset.noOffset)
    
## Configuration

The default settings can be changed with the configuration properties defined in
@ref:[reference.conf](configuration.md#default-configuration).

Query configuration is under `akka.persistence.cassandra.query`.

Events by tag configuration is under `akka.persistence.cassandra.events-by-tag` and shared
b `journal` and `query`.
