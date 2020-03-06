# Java Driver Code Samples

A list of standalone working class to illustrate one feature of the java driver. We implemented those for both driver 3.x (oss) and driver 4.x (merged drivers). 

Contributors:
- Cedrick Lunven (@clun)
- Eric Ramirez 

## Objectives

We want to provide an extended list of code samples in a sample place with explicit names to speed up you devs with copy-paste.


## Setup and Running

### Prerequisites

* Java 11+
* Cassandra installed locally *(we provide a `docker-compose` if you don't have cassandra installed)*
* OR Docker if Cassandra is not installed locally, we provide a `docker-compose` if you don't have cassandra installed

### Running

**Start Cassandra as a container:** After cloning this repository simply do

```
docker-compose up -d
```

**Having CQLSH:** If cassandra is running as a docker container and you want to have a cqlsh shell please execute:

```
docker exec -it `docker ps | grep cassandra:3.11.5 | cut -b 1-12` cqlsh
```

Then you can execute each `SampleCode???` as a standalone class or eventually run with maven


### Schema Created

Proposed Schema ffor the different samaples above

```sql

// ----------------------------------------
// Sample Keyspace (to be used locally)
// 
// Here a sample if you want to create on multiple node DC
// CREATE KEYSPACE IF NOT EXISTS killrvideo 
// WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'datacenter1' : 3 }
// AND DURABLE_WRITES = true;
// ----------------------------------------

CREATE KEYSPACE IF NOT EXISTS killrvideo 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
AND DURABLE_WRITES = true;

// ----------------------------------------
// Basic table for basic operations
// ----------------------------------------
// Used by : SampleCodeXx_CRUD_01_Simple
// Used by : SampleCodeXx_CRUD_02_Paging
// Used by : SampleCodeXx_CRUD_06_Async
// Used by : SampleCodeXx_CRUD_09_LightweightTransactions
// ----------------------------------------

CREATE TABLE IF NOT EXISTS users (
 email      text,
 firstname  text,
 lastname   text,
 PRIMARY KEY (email)
);


// ----------------------------------------
// Table to show MAP, LIST, SET, UDT, JSON
// ----------------------------------------
// Used by : SampleCodeXx_CRUD_04_ListSetMapAndUdt
// Used by : SampleCodeXx_CRUD_06_Json
// ----------------------------------------

CREATE TYPE IF NOT EXISTS video_format (
  width   int,
  height  int
);

CREATE TABLE IF NOT EXISTS videos (
  videoid    uuid,
  title      text,
  upload     timestamp,
  email      text,
  url        text,
  tags       set <text>,
  frames     list<int>,
  formats    map <text,frozen<video_format>>,
  PRIMARY KEY (videoid)
);


// ----------------------------------------
// Table to show Batches, ObjectMapping
// ----------------------------------------
// Used by : SampleCodeXx_CRUD_03_Bacthes
// Used by : SampleCodeXx_CRUD_07_ObjectMapping
// ----------------------------------------

CREATE TABLE IF NOT EXISTS comments_by_video (
    videoid uuid,
    commentid timeuuid,
    userid uuid,
    comment text,
    PRIMARY KEY (videoid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);

CREATE TABLE IF NOT EXISTS comments_by_user (
    userid uuid,
    commentid timeuuid,
    videoid uuid,
    comment text,
    PRIMARY KEY (userid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);


// ----------------------------------------
// Table to show Counters
// ----------------------------------------
// Used by : SampleCodeXx_CRUD_08_Counters
// ----------------------------------------

CREATE TABLE IF NOT EXISTS videos_views (
    videoid     uuid,
    views       counter,
    PRIMARY KEY (videoid)
);


// ----------------------------------------
// Table to show Binary DATA
// ----------------------------------------
// Used by : SampleCodeXx_CRUD_10_Blob
// ----------------------------------------

CREATE TABLE IF NOT EXISTS files (
   filename  text,
   upload    timestamp,
   extension text static,
   binary    blob,
   PRIMARY KEY((filename), upload)
) WITH CLUSTERING ORDER BY (upload DESC);


```

## Sample code



