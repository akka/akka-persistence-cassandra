# Query Plugin

It implements the following @extref:[Persistence Queries](akka:scala/persistence-query.html):

* persistenceIds, currentPersistenceIds
* eventsByPersistenceId, currentEventsByPersistenceId
* eventsByTag, currentEventsByTag

Persistence Query usage example to obtain a stream with all events tagged with "someTag" with Persistence Query:

    val queries = PersistenceQuery(system).readJournalFor[CassandraReadJournal](CassandraReadJournal.Identifier)
    queries.eventsByTag("someTag", Offset.noOffset)
    
## Cleanup of tag_views table

By default the tag_views table keeps tagged events indefinitely, even when the original events have been removed. 
Depending on the volume of events this may not be suitable for production.

Before going live decide a time to live (TTL) and, if small enough, consider using the [Time Window Compaction Strategy](http://thelastpickle.com/blog/2016/12/08/TWCS-part1.html).
See `events-by-tag.time-to-live` in reference.conf for how to set this.

