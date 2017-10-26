# Events by tag

WIP: This documents the new implementation, the existing one uses Cassandra Materialized
views which have been reported to unstable. See [here](https://github.com/akka/akka-persistence-cassandra/issues/247)

Events by tag is a hard query to execute efficiently in Cassandra. This page documents
how it works and what are the consistency guarantees.

## Bucket sizes 

Another improvement the new eventByTag implementation will have is a configurable bucket so that
high throughput tags are supported.

 1000s of events per day per tag -- DAY
 100,000s  of events per day per tag -- HOUR
 1,000,000s of events per day per tag -- MINUTE
 
More billion per day per tag? You probably need a specialised schema rather than a general library like this.

### Migrating

A one off manual run of a provided stream will take events from the messages table and
save them to the tag views table. This may take a long time if you have a lot of events.

## Consistency

Event by tag queries are eventually consistent, i.e. if a persist for an event has completed
is does not guarantee that it will be visible in an events by tag query, but it will eventually.

Order of the events is maintained per persistence id per tag by maintaining a sequenceId for tagged events.

### Missing and delayed events.

Cassandra does not offer isolation in a batch even for the same partition (whatever the docs might say).
If an eventsByTag query is happening at the same time as a batch insert to `tag_views` table then it might
see an events from the same persistence id and tag in the incorrect order.

For that reason the `tag_views` table has an additional sequence number maintained for every 
persistenceId, tag combination. This sequence number can be generated as all the events for a given
persistenceId are generated on the same node.

If a gap is detected then the event is held back and the stream goes into a searching mode for the missing
events. Depending on whether it is a live query and/or you have set an offset it might be possible
to know for sure that an event is missing or it is just before the offset.

Details for each of the four combinations of current/live and off set/no offset:
* First event missing: The first found does not have sequence number 1
* Event missing: Find some events in order then a sequence number jump
* Omitted events: Can events be omitted or will the stream fail


#### Current query / No offset

* First event missing: Back track to `first-time-bucket`. N scans of each bucket.
* Event missing: N scans from the previous event for this persistenceId/tag's offset to the current offset.
* Omitted events: No, stream will fail if either of the above two scenarios don't find the missing event.

#### Current query / Offset

* First event missing: Back track to Offset. N scans of each bucket then assume it is the first event from this offset.
* Event missing: N scans from the previous event for this persistenceId/tag's offset to the current offset.
* Omitted events: Yes. If first event missing doesn't find an event that should be there.


#### Live query / No Offset

* First event missing: Back track to `first-time-bucket`. N scans of each bucket.
* Event missing: N scans from the previous event for this persistenceId/tag's offset to the current offset.
* Omitted events: No, stream will fail if either of the above two scenarios don't find the missing event.

#### Live query / Offset

* First event missing: Back track to Offset. N scans of each bucket then assume it is the first event from this offset.
* Event missing: N scans from the previous event for this persistenceId/tag's offset to the current offset.
* Omitted events: Yes. If first event missing doesn't find an event that should be there.

## Implementation

A `TagWriter` actor is created, on demand, per tag that batches (unlogged batch grouped by the partition key)
up writes to a separate table partitioned by tag name and a time bucket.

When a message is persisted, if it has tags, it is sent to each of the `TagWriter`s asynchronously.

This allows us to be more efficient and thus support more tags than the old version (limited to 3). No limit is 
imposed with new implementation but it is advised to keep it small i.e. less than 10.


### Crash recovery

`TagWriter` maintains a separate table with how far it has gotten up to
for each persistenceId.

```
CREATE TABLE akka.tag_views_progress (                              
    peristence_id text,           
    tag text,                     
    sequence_nr bigint,           
    PRIMARY KEY (peristence_id, tag)                                
)
```

Part of recovery will then check that all events have been written by the tag writer.

On the happy path this table will be written to after the tag_view write is complete
so it could be lost in a crash. In that event recovery will duplicate writes to the 
tag_views table which is fine as they will be upserts.

TODO:
What happens if a persistenceId is not uses for a long time after a crash? eventsByTag queries 
would remain out of date for a long time.

### Deletion of events

*Deletes are not propagated to the events by tag table.*

It is assumed that the eventsByTag query is used to populate a separate read view or
events are never deleted. An optional TTL can be set on the tag_view table to clean up
events assuming they have been written to another read view database, otherwise they are
kept forever.

