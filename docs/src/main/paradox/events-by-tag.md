# Events by tag

The events by tag query is implemented by writing the events to another table in Cassandra partitioned by tag as well
as a timebucket to prevent partitions becoming too big.

Due to it being a separate table and events are written from multiple nodes the events by tag is eventually consistent.

## Consistency

Event by tag queries are eventually consistent, i.e. if a persist for an event has completed
is does not guarantee that it will be visible immediately but it will eventually.

Order of the events is maintained per persistence id per tag by maintaining a sequence number for tagged events per persistence id.
This is in addition to the normal sequence nr kept per persistence id. Events for the same persistence id will never be delivered out
of order.

Events from different persistent IDs are ordered by a 
[type 2 UUID a.k.a TimeUUID](https://en.wikipedia.org/wiki/Universally_unique_identifier),
exposed in the `EventEnvelope` as a `TimeBasedUUID` offset.

As events come from multiple nodes to deliver events in `TimeUUID` order there is an `eventual-consistency-delay` to give
events time to arrive in Cassandra and for the query to deliver them in their final order. The `eventual-consistency-delay` keeps the query a configurable 
amount of time in the past.

The events by tag stream updates the Cassandra query it uses to start from the latest offset to avoid reading events it has already delivered. 
If events are delayed and the query progresses past an event's offset it will be detected either when the next event for that 
persistence id is received or if no more events are received for the persistence id then it will be detected via [back tracking](#back-tracking). 

In both cases the query will go back and search for the delayed events. The search for missing events is an expensive operation
so it is advised not to set the `eventual-consistency-delay` too low as it increased the liklihood of missed events.

@@@ warning

Back tracking only goes back to the offset the query is started from. Meaning that if a query is restarted from an offset and there have been events
delayed greater than the `eventual-consistency-delay` those events will not be delivered.

@@@

### What can delay events?

Events can be delayed for many reasons:

* GC pause between serializing the event in the Akka node and writing it to the database
* Event waiting to be written as part of a batch to the `tag_views` table
* Out of sync clocks
* Overloaded database

#### Overloaded database

Typical write timeouts for Cassandra are between 2 and 10 seconds. If these timeouts are hit when writing to the `tag_views` table
it means that by the time the write is retried the event will have been delayed by 2-10 seconds while the original write timed out.

Normally these delayed events will be picked up by [back tracking](#back-tracking) but if the events by tag query is restarted from a later
offset then back tracking won't see these delayed events.

## Back tracking

If another event for the same persistence id is received the sequence number is used to detect that an event has been missed.
To detect missed events without receiving another event for the same persistence id a periodic backtrack is done
to find events. Queries are run in the background to detect delayed events. A high frequency short back track is done
for finding events delayed a small amount and a low frequency backtrack that scans further back. 

These are configured with `akka.persistence.cassandra.events-by-tag.back-track`:

@@snip [reference.conf](/core/src/main/resources/reference.conf) { #backtrack }                                                                                                                                

## Tuning for lower latency

The default `eventual-consistency-delay` is set to `5s` which is high as it is the safest. This can be set much lower 
for latency sensitive applications and missed events will be detected via the above mechanisms. The downside is that
the search for missing events is an expensive operation and with a high eventual consistency delay it won't happen without exceptional delays.
With a low value it will happen frequently.

Like with any tuning testing should be done to see what works for your environment. The values below may be too low
depending on the load and performance of your Cassandra cluster.

It is possible to flush the tag writes right away. By default they are batched and written as an unlogged batch which increases throughput. To write each individually
without delay use:
```
akka.persistence.cassandra.events-by-tag.flush-interval = 0s
```

Alternatively set a very small value e.g. `25ms` so some batching is done. If your application has a large number of tagged events per second
it is highly advised to set this to a value above 0.

Enable pub sub notifications so events by tag queries can execute a query right away rather than waiting for the next
`refresh-interval`. Lower the `refresh-interval` for cases where the pub sub messages take a long time to arrive at the
query.
```
akka.persistence.cassandra.events-by-tag.pubsub-notification = on
akka.persistence.cassandra.query.refresh-interval = 2s
```

Reduce `eventual-consistency-delay`. You must test this has positive results for your use case. Setting this too low
can decrease throughput and latency as more events will be missed initially and expensive searches carried out.

```
akka.persistence.cassandra.events-by-tag.eventual-consistency-delay = 50ms
```

## Missing searches and gap detection

If a gap is detected then the event is held back and the stream goes into a searching mode for the missing
events. The search only looks in the current bucket and the previous one. If the event is older then it won't be found in this search.

If the missing event is not found then the stream is failed unless it is the first time the persistence id
has been encountered by the query.

### Gap detection when encountering a persistence id for the first time

When receiving the first event for a given persistence id in an offset query it is not known 
if that is actually the first event. If the offset query starts in a time bucket in the past then
it is assumed to be the first. 

If the query is started with an offset in the current time bucket then
the query starts with a search to look for the lowest sequence number per persistenceId
before delivering any events.
 
The above scanning only looks in the current time bucket. For persistence ids that aren't found in this initial scan because they 
don't have any events in the current time bucket then the expected tag pid sequence number is not known. 
In this case the eventsByTag query searches for `new-persistence-id-scan-timeout` before assuming this is the first
event for that persistence id. 

This adds a delay each time a new persistence id is found by an offset query when the first event doesn't have a sequenceNr of `1`. 
If this is an issue it can be set to 0s. If events are found out of order due to this the stage will fail.  

## Events by tag reconciliation

In the event that the `tag_views` table gets corrupted there is a @apidoc[akka.persistence.cassandra.reconciler.Reconciliation]
tool that can help fix it. It can only be run while the application is offline but per persistence id operations can
be used if it is known that the persistence id is not running.

It supports:

* Deleting all events in the tag view for a given persistence id
* Re-building the tag view for a persistence id for a tag
* Query for all tags (inefficient query)
* Truncate the tag view and all metadata so it can be re-built

After deleting the tag views they will be automatically re-built next time the persistence id starts or with an explicit rebuild.

For example, to rebuild the data for a persistence id:

@@snip [reconciler](/core/src/test/scala/doc/reconciler/ReconciliationCompileOnly.scala) { #imports #reconcile}                                                                                                                                

## Other tuning

### Setting a bucket size

Events by tag partitions are grouped into buckets to avoid a single partition for a tag
getting too large. 

* 1000s of events per day per tag -- Day
* 100,000s  of events per day per tag -- Hour
* 1,000,000s of events per day per tag -- Minute
 
The default is `Hour` and can be overridden by setting `akka.persistence.cassandra.events-by-tag.bucket-size`.
This setting can not be changed after data has been written.
 
More billion per day per tag? You probably need a specialised schema rather than a general library like this.

If a Cassandra partition gets too large it will perform badly (and has a hard limit of 2 billion cells, a cell being similar to a column). 
Given there is ~10 columns in the table there was a hard limit previously of 200million but performance would degrade before reaching this. 

The support for `Minute` bucket size means that there are 1440 partitions per day. 
Given a large C* cluster this should work until you have in the order of 10s of millions per minute
when the write rate to that partition would likely become the bottleneck rather than partition size.

The increased scalability comes at the cost of having to query more partitions on the read side so don't 
use `Minute` unless necessary.

### Setting a write batch size

The size of the of the batch is controlled via `max-message-batch-size`. Cassandra imposes a limit on the serialized size
of a batch, currently 50kb by default. The larger this value the more efficient the tag writes will be but care must 
be taken not to have batches that will be rejected by Cassandra. Two other cases cause the batch to be written before the batch size is reached:

* Periodically: By default 250ms. To prevent eventsByTag queries being too out of date.
* When a starting a new timebucket, which translates to a new partition in Cassandra, the events for the old timebucket are written.

## Cleanup of tag_views table

By default the tag_views table keeps tagged events indefinitely, even when the original events have been removed. 
Depending on the volume of events this may not be suitable for production.

Before going live decide a time to live (TTL) and, if small enough, consider using the [Time Window Compaction Strategy](http://thelastpickle.com/blog/2016/12/08/TWCS-part1.html).
See `events-by-tag.time-to-live` in reference.conf for how to set this.

@@@ warning

The TTL must be greater than any expected delay in using the tagged events to build a read side view
otherwise they'll be deleted before being read.

@@@

The @apidoc[akka.persistence.cassandra.cleanup.Cleanup] tool can also be used for deleting from the `tag_views`
table. See @ref[Database Cleanup](./cleanup.md) for more details.

## How it works

The following section describes more about how the events by tag query work, it is not required knowledge
for using it.

A separate table is maintained for events by tag queries to that is written to in batches.

The table is partitioned via tag and a time bucket. 
This allows events for the same tag can be queried and the time bucket prevents all events
for the same tag being in a single partition.
 
The timebucket causes a hot spot for writes but
this is kept to a minimum by making the bucket small enough and by batching the writes into
unlogged batches.

Each events by tag query starts at the provided offset and queries the events. 
The stream will initially do a query per time bucket to get up to the current 
(many of these could be empty so very little over head, some may bring back many events). For example
if you set the offset to be 30 days ago and a day time bucket, it will require 30 * nr queries to get each stream up to date. 
If a particular partition contains many events then it is paged back from Cassandra based on demand.

When no offset is provided EventsByTag queries start at the configured `events-by-tag.first-time-bucket`. For new applications
increase this to the day your application is initially deployed to speed up `NoOffset` queries.

### Implementation

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
so it could be lost in a crash. 

#### Recovery/Restart scenarios

On recovery the tag write progress for a given pid / tag is sent to the tag writer so that
it bases its tag pid sequence nrs off the recovery. Following scenarios:

No event buffered in the tag writer. Tag write progress has been written.

* Recovery will see that no events needs to be recovered

No events buffered in the tag writer. Tag write progress is lost.

* Events will be in the `tag_views` table but not the `tag_write_progress`.
* Tag write progress will be out of date
* Events will be recovered and sent to the tag writer, should receive the same tag pid sequence nr and be upserted.

Events buffered in the tag writer. 

* Buffered events for the persistenceId should be dropped as if they are buffered the tag write progress
won't have been saved as it happens after the write of the events to tag_views.
