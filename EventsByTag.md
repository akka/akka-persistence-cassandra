# Events by tag

WIP: This documents the new implementation, the existing one uses Cassandra Materialized
views which have been reported to unstable. See [here](https://github.com/akka/akka-persistence-cassandra/issues/247)

Events by tag is a hard query to execute efficiently in Cassandra. This page documents
how it works and what are the consistency guarantees.

## Bucket sizes 

Events by tag partitions are grouped into buckets to avoid a partition for a given tag
getting to large. 

 1000s of events per day per tag -- DAY
 100,000s  of events per day per tag -- HOUR
 1,000,000s of events per day per tag -- MINUTE
 
This setting can not be changed after data has been written.
 
More billion per day per tag? You probably need a specialised schema rather than a general library like this.

### Justification for bucket sizes

The old implementation put all tags for each day in a single cassandra partition. 
If a cassandra partition gets too large it starts to perform badly (and has a hard limit of 2 billion cells, a cell being similar to a column). 
Given we have ~10 columns in the table there was a hard limit previously of 200million but it would have performed terribly before getting any where near this.

The support for `Minute` bucket size means that there are 1440 partitions per day. 
Given a large C* cluster this should work until you have in the order of 10s of millions per minute
when the write rate to that partition could become the bottleneck rather than partition size.

The increased scalability comes at the cost of having to query more partitions on the read side so don't 
use `Minute` unless necessary.

### Migrating

A one off manual run of a provided stream will take events from the messages table and
save them to the tag views table. This can run while your application is running but be careful 
as it will produce a large number of writes to Cassandra. 

Once you restart with the new version of the plugin each time a persistent actor is recovered
it will repair the tag views table for any event that was missed during the migration.

## Consistency

Event by tag queries are eventually consistent, i.e. if a persist for an event has completed
is does not guarantee that it will be visible in an events by tag query, but it will eventually.

Order of the events is maintained per persistence id per tag by maintaining a sequence number for tagged events.
If an event is missing then `eventByTag` queries will fail.

### Missing and delayed events.

Cassandra does not offer isolation in a batch even for the same partition (whatever the docs might say).
If an eventsByTag query is happening at the same time as a batch insert to `tag_views` table then it might
see an events from the same persistence id and tag in the incorrect order.

For that reason the `tag_views` table has an additional sequence number maintained for every 
persistenceId, tag combination. This sequence number can be generated as all the events for a given
persistenceId are generated on the same node.

If a gap is detected then the event is held back and the stream goes into a searching mode for the missing
events. If the missing event is not found then then stream is failed.

When receiving the first event for a given persistenceId in an offset query it is not known 
if that is actually the first event. If the offset query starts in a time bucket in the past then
it is assumed to be the first. If the query is started with an offset in the current time bucket then
the query starts with a scanning mode to look for the lowest sequence number per persistenceId
before delivering any events. 

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

Part of recovery checks that all events have been written by the tag writer.

On the happy path this table will be written to after the tag_view write is complete
so it could be lost in a crash. In that event recovery will duplicate writes to the 
tag_views table which is fine as they will be upserts.

### Deletion of events

*Deletes are not propagated to the events by tag table.*

It is assumed that the eventsByTag query is used to populate a separate read view or
events are never deleted. A future feature will be to TTL the table. 

