# Events by tag

This documents how EventsByTag works from version 0.80, older versions use Cassandra Materialized
views which have been reported to unstable. See [here](https://github.com/akka/akka-persistence-cassandra/issues/247)

Events by tag is a hard query to execute efficiently in Cassandra. This page documents
how it works and what are the consistency guarantees.

## How it works

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

When no offset is provided EventsByTag queries start at the configured `first-time-bucket`. For new applications
increase this to the day your application is initially deployed to speed up `NoOffset` queries.

## Bucket sizes 

Events by tag partitions are grouped into buckets to avoid a single partition for a tag
getting to large. 

* 1000s of events per day per tag -- DAY
* 100,000s  of events per day per tag -- HOUR
* 1,000,000s of events per day per tag -- MINUTE
 
This setting can not be changed after data has been written.
 
More billion per day per tag? You probably need a specialised schema rather than a general library like this.

If a cassandra partition gets too large it will perform badly (and has a hard limit of 2 billion cells, a cell being similar to a column). 
Given there is ~10 columns in the table there was a hard limit previously of 200million but performance would degrade before reaching this. 

The support for `Minute` bucket size means that there are 1440 partitions per day. 
Given a large C* cluster this should work until you have in the order of 10s of millions per minute
when the write rate to that partition would likely become the bottleneck rather than partition size.

The increased scalability comes at the cost of having to query more partitions on the read side so don't 
use `Minute` unless necessary.

## Write batch size

The size of the of the batch is controlled via `max-message-batch-size`. Cassandra imposes a limit on the serialized size
of a batch, currently 50kb by default. The larger this value the more efficient the tag writes will be but care must 
be taken not to have batches that will be rejected by Cassandra. Two other cases cause the batch to be written before the batch size is reached:
* Periodically: By default 250ms. To prevent eventsByTag queries being too out of date.
* When a starting a new timebucket, which translates to a new partition in Cassandra, the events for the old timebucket are written.


## Consistency

Event by tag queries are eventually consistent, i.e. if a persist for an event has completed
is does not guarantee that it will be visible in an events by tag query, but it will eventually.

Order of the events is maintained per persistence id per tag by maintaining a sequence number for tagged events per persistence id.
This is in addition to the normal sequence nr kept per persistence id.
This additional sequence number is used to detect missing events and trigger searching back. However this only works if
a new event for the same persistenceId is encountered. If your usecase involves many persistenceIds with only a small 
number of events per persistenceId then the `eventual-consistency-delay` is essential to mitigate the risk of missing
delayed events in live queries.

### Gap detection

If a gap is detected then the event is held back and the stream goes into a searching mode for the missing
events. The search only looks in the current bucket and the previous one. If the event is older then it won't be found in this search.

If the missing event is not found then the stream is failed unless it is the first time the persistence id
has been encountered by the query (see below).

The stream is failed with a `MissingTaggedEventException` which has a field `lastKnownOffset`. The query can be restarted
from this offset to search further back than one bucket for the missing event. However, previously delivered events with an offset
greater than the `lastKnownOffset` will be re-delivered so downstream processing must be idempotent.

### Gap detection when encountering a persistence id for the first time

When receiving the first event for a given persistenceId in an offset query it is not known 
if that is actually the first event. If the offset query starts in a time bucket in the past then
it is assumed to be the first. 

If the query is started with an offset in the current time bucket then
the query starts with a scanning mode to look for the lowest sequence number per persistenceId
before delivering any events.
 
The above scanning only looks in the current time bucket. For persistenceIds that aren't found in this initial scan because they 
don't have any events in the current time bucket then the expected tag pid sequence number is not known. 
In this case the eventsByTag query looks for the `new-persistence-id-scan-timeout`. This adds a delay each time a new persistenceId
is found by an offset query when the first event doesn't have a sequenceNr of `1`. 
If this is an issue it can be set to 0s. If events are found out of order due to this
the stage will fail.  

### Delayed events and out of sync clocks

Events for a tag are globally ordered using a TimeUUID.
However events for the same tag but for a difference persistenceIds can come from different nodes in a cluster meaning that
events enter Cassandra out of TimeUUID order. 

If a live eventsByTag query is currently running it can see newer events before the older delayed events. For
that reason an `eventual-consistency-delay` is used to keep eventsByTag a configurable amount of time in the past to give a time
for events from all nodes to settle to their final order.

Setting this to a small value can lead to:

* Receiving events out of TimeUUID (offset) order for different persistenceIds, meaning if the offset is saved for restart/resume then delayed events can be missed on restart 
* Increased likelihood of receiving events for the same persistenceId out of order. This is detected but the stream is temporarily paused to search for the missing events which is less efficient then reading them initially in order.
* Missing events for persistence ids the query instance sees for the first time (unless it is tag pid sequence number 1) due to the query not knowing which tag pid sequence nr to expect.

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

### Deletion of events

*Deletes are not propagated to the events by tag table.*

It is assumed that the eventsByTag query is used to populate a separate read view or
events are never deleted.

