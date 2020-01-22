# Events by tag reconciliation 

In the event of a split brain or some other cause of the `tag_views`s table being
corrupted e.g. a bug in the journal, it may be necessary to re-build the `tag_view` table.

1. Stop the application
1. Truncate the tag related tables:
  ```
  TRUNCATE tag_views;
  TRUNCATE tag_write-progress; 
  TRUNCATE tag_scanning;
  ```
1. 

