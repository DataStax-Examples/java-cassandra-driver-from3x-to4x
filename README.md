# Java Driver Code Samples

<img src="https://raw.githubusercontent.com/clun/java-cassandra-driver-from3x-to4x/master/example-3x/src/main/resources/cassandra_logo.png" height="120px" />


## Objectives

This repository contains a list of standalone classes illustrating each one dedicated feature of the *DataStax java driver*. The purpose is to provide you an extended list of code samples with explicit names to speed up you developments with copy-paste.

We implemented those for both driver 3.x (oss) and driver 4.x

## Contributors

* [Cedrick Lunven](https://github.com/clun)
* [Eric Ramirez](https://github.com/flightc) 


## Setup and Running

### Prerequisites

* Java 11+
* **Cassandra installed locally** OR  Docker *(we provide a `docker-compose.yaml`)*
* Maven to compile and eventually run the samples (OR your IDE)


### Running

** Start Cassandra ** 

After cloning this repository you can start either you local instance of Cassandra with `$Cassandra_HOME/bin/cassandra` or with docker-compose.

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



