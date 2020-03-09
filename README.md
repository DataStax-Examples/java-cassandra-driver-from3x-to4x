# Java Driver Code Samples

<img src="https://raw.githubusercontent.com/clun/java-cassandra-driver-from3x-to4x/master/example-3x/src/main/resources/cassandra_logo.png" height="120px" />


## Objectives

This repository contains a list of standalone classes illustrating each a dedicated feature of the *DataStax java driver*.

The purpose is to provide you an extended list of code samples with explicit names to speed up you developments (with copy-paste). We implemented those for both driver 3.x *(previous oss)* and driver 4.x *(latest)*

## Contributors

* [Cedrick Lunven](https://github.com/clun)
* [Eric Ramirez](https://github.com/flightc) 


## Setup and Running

### Prerequisites

* `Java` 11+
* `Cassandra` installed locally **OR**  `Docker` *(we provide you a working `docker-compose.yaml`)*
* `Maven` to compile and eventually run the samples (OR your IDE)


### Running

* **Start Cassandra** 

After cloning this repository you can start either you local instance of Cassandra with `$Cassandra_HOME/bin/cassandra` or with docker-compose.

```
docker-compose up -d
```

* **Run Samples** 

You can execute each class with `maven` and or your favorite IDE. Each class will create everything needed each time `keyspace` and `tables`. The working tables will be empty in the beginning for not dropped.

```
cd example-3x
mvn exec:java -D"exec.mainClass"="com.datastax.samples.SampleCode3x_CONNECT_ClusterShowMetaData"
```

* **Data displayed with CQL Shell** 

If cassandra is running as a docker container and you want to have a cqlsh shell please execute:

```
docker exec -it `docker ps | grep cassandra:3.11.5 | cut -b 1-12` cqlsh
```

## Working with Schema

|       3x        |       4x       |  Description        |
| :-------------: |:-------------:|:---------------------|
| [ShowMetaData3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CONNECT_ClusterShowMetaData.java) | [ShowMetaData4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CONNECT_ClusterShowMetaData.java) |  Connect to cluster then show keyspaces and metadata |
| [CreateKeyspace3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CONNECT_CreateKeyspace.java) | [CreateKeyspace4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CONNECT_CreateKeyspace.java) |  Create the `killrvideo` keyspace using `SchemaBuilder` if not exist |
| [CreateSchema3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CONNECT_CreateSchema.java) | [CreateSchema4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CONNECT_CreateSchema.java) |  Create `table` and `type` in `killrvideo` keyspace if they don't exist |
| [DropKeyspace3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CONNECT_DropKeyspace.java) | [DropKeyspace4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CONNECT_DropKeyspace.java) |  Drop the `killrvideo` keyspace if existis using  `SchemaBuilder` |
| [DropSchema3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CONNECT_DropSchema.java) | [DropSchema4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CONNECT_DropSchema.java) |  Drop all  `table` and `type` in `killrvideo` keyspace if they exist |


## Executing Queries

|       3x        |       4x      |  Description        |
| :-------------  |:------------- |:---------------------|
| [GettingStarted3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CRUD_00_GettingStarted.java) | [GettingStarted4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CRUD_00_GettingStarted.java) |  First touch with executing queries |
| [Simple3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CRUD_01_Simple.java) | [Simple4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CRUD_01_Simple.java) |  Read, update, insert, delete operations using `QueryBuilder` |
| [Paging3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CRUD_02_Paging.java) | [Paging4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CRUD_02_Paging.java) |  Illustrating FetchSize and how to retrieve page by page |
| [Batches3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CRUD_03_Batches.java) | [Batches4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CRUD_03_Batches.java) |  Group statements within batches|
| [ListSetMapUdt3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CRUD_04_ListSetMapAndUdt.java) | [ListSetMapUdt4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CRUD_04_ListSetMapAndUdt.java) |  Advanced types insertions with `list`, `set`, `map` but also `User Defined Type` |
| [Json3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CRUD_05_Json.java) | [Json4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CRUD_05_Json.java) |  Work with columns or full record with `JSON` |
| [Async3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CRUD_06_Async.java) | [Async4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CRUD_06_Async.java) |  Sample operations as Simple in `Asynchronous` way |
| [ObjectMapping3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CRUD_07_ObjectMapping.java) | [ObjectMapping4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CRUD_07_ObjectMapping.java) | Map table record to Java POJO at driver level |
| [Counter3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CRUD_08_Counters.java) | [Counter4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CRUD_08_Counters.java) |  Working with `counters` increment/decrement|
| [Lwt3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CRUD_09_LightweightTransactions.java) | [Lwt4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CRUD_09_LightweightTransactions.java) |  Working for Lightweight transactions read-before-write|
| [BlobAndCodec3x](./example-3x/src/main/java/com/datastax/samples/SampleCode3x_CRUD_10_BlobAndCodec.java) | [BlobAndCodec4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CRUD_10_BlobAndCodec.java) |  Working with `BLOB` and binary data but also how to create your own `CustomCodec` |
| [N/A](#) | [Reactive4x](./example-4x/src/main/java/com/datastax/samples/SampleCode4x_CRUD_11_Reactive.java) |  Working with the Reactive API introduce in driver 4.x|


For reference this is the working schema we used for queries

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


