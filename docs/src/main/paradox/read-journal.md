# Query Plugin

It implements the following @extref:[Persistence Queries](akka:scala/persistence-query.html):

* eventsByPersistenceId, currentEventsByPersistenceId
* eventsByTag, currentEventsByTag
* persistenceIds, currentPersistenceIds 

Due to the nature of Cassandra the query for `persistenceIds` is inefficient and unlikely
to work for use cases with large amounts of data.

Persistence Query usage example to obtain a stream with all events tagged with "someTag" with Persistence Query:

    val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
    queries.eventsByTag("someTag", Offset.noOffset)
    
## Configuration 

### Query settings

Under `akka.persistence.cassandra.query`:

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #query }

#### Cassandra driver overrides

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #profile }

#### Shared settings for all parts of the plugin

The following settings are shared by the `journal`, `query`, and `snapshot` parts of the plugin and are under
`akka.persistence.cassandra`: 

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #shared }

Events by tag configuration is under `akka.persistence.cassandra.events-by-tag` and shared
b `journal` and `query`.

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #events-by-tag }




    
