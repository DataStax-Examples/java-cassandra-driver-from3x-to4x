package com.datastax.samples;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder.Direction;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Code reused in multiple samples.
 *
 */
public class ExampleUtils implements ExampleSchema {

    private static Logger LOGGER = LoggerFactory.getLogger(ExampleUtils.class);
    
    public static Session connect(Cluster cluster) {
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        Session session = cluster.connect(KEYSPACE_NAME);
        LOGGER.info("[OK] Connected to Keyspace '{}'", KEYSPACE_NAME);
        return session;
    }
    
    public static void closeSessionAndCluster(Session session, Cluster cluster) {
        if (session != null) session.close();
        if (cluster != null) cluster.close();
        LOGGER.info("[OK] Cluster and Session are now closed");
    }
    
    public static void truncateTable(Session session, String tableName) {
        session.execute(QueryBuilder.truncate(tableName));
    }
    
    public static void dropTableIfExists(Session session, String tableName) {
        session.execute(SchemaBuilder.dropTable(tableName).ifExists());
    }
    
    public static void dropTypeIffExists(Session session, String typeName) {
        session.execute(SchemaBuilder.dropType(typeName).ifExists());
    }
    
    /**
     * CREATE KEYSPACE killrvideo 
     * WITH replication = 
     *      {'class': 'SimpleStrategy', 
     *       'replication_factor': '1'}  
     * AND durable_writes = true;
     */
    private static Statement createKeyspaceSimpleStrategy(String keyspaceName, int replicationFactor) {
        return SchemaBuilder.createKeyspace(keyspaceName)
                .ifNotExists().with().durableWrites(true)
                .replication(ImmutableMap.of(
                    "class", "org.apache.cassandra.locator.SimpleStrategy",
                    "replication_factor", replicationFactor));
    }
    
    public static void createKeyspace(Cluster cluster) {
        LOGGER.info("Connected to Cassandra Cluster");
        try(Session session = cluster.connect()) {
            session.execute(createKeyspaceSimpleStrategy(KEYSPACE_NAME, KEYSPACE_REPLICATION_FACTOR));
            LOGGER.info("+ Keyspace '{}' created (if needed).", KEYSPACE_NAME);
        }
    }
    
    public static void createKeyspace() {
        try(Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build()) {
            createKeyspace(cluster);
        }
    }
    
    /**
     * CREATE TABLE IF NOT EXISTS users (
     *  email      text,
     *  firstname  text,
     *  lastname   text,
     *  PRIMARY KEY (email)
     * );
     */
    public static void createTableUser(Session session) {
        SchemaStatement userStmt = SchemaBuilder
                .createTable(USER_TABLENAME).ifNotExists().
                 addPartitionKey(USER_EMAIL, DataType.text()).
                 addColumn(USER_FIRSTNAME, DataType.text()).
                 addColumn(USER_LASTNAME, DataType.text());
        session.execute(userStmt);
        LOGGER.info("+ Table '{}' has been created (if needed).", USER_TABLENAME);
    }
    
    /**
     * CREATE TYPE IF NOT EXISTS video_format (
     *   width   int,
     *   height  int
     *);
     */
    public static void createUdtVideoFormat(Session session) {
        SchemaStatement userStmt = SchemaBuilder
                .createType(UDT_VIDEO_FORMAT_NAME).ifNotExists()
                .addColumn(UDT_VIDEO_FORMAT_WIDTH,  DataType.cint())
                .addColumn(UDT_VIDEO_FORMAT_HEIGHT, DataType.cint());
        session.execute(userStmt);
        LOGGER.info("+ Type '{}' has been created (if needed).", UDT_VIDEO_FORMAT_NAME);
    }
    
    /**
     * CREATE TABLE IF NOT EXISTS videos (
     *   videoid   uuid,
     *   title     text,
     *   upload    timestamp,
     *   email     text,
     *   url       text,
     *   tags      set <text>,
     *   frames    list<int>,
     *   formats   map <text,frozen<video_format>>,
     *   PRIMARY KEY (videoid)
     * ); 
     **/
    public static void createTableVideo(Session session) {
        SchemaStatement userStmt = SchemaBuilder
                .createTable(VIDEO_TABLENAME).ifNotExists().
                 addPartitionKey(VIDEO_VIDEOID, DataType.uuid()).
                 addColumn(VIDEO_TITLE, DataType.text()).
                 addColumn(VIDEO_UPLOAD, DataType.timestamp()).
                 addColumn(VIDEO_USER_EMAIL, DataType.text()).
                 addColumn(VIDEO_URL, DataType.text()).
                 addColumn(VIDEO_TAGS, DataType.set(DataType.text())).
                 addColumn(VIDEO_FRAMES,  DataType.list(DataType.cint())).
                 addUDTMapColumn(VIDEO_FORMAT, DataType.text(), SchemaBuilder.frozen(UDT_VIDEO_FORMAT_NAME));
        session.execute(userStmt);
        LOGGER.info("+ Table '{}' has been created (if needed).", VIDEO_TABLENAME);
    }
   
    /**
     * CREATE TABLE IF NOT EXISTS files (
     *  filename  text,
     *  upload    timestamp,
     *  extension text static,
     *  binary    blob,
     *  PRIMARY KEY((filename), upload)
     * ) WITH CLUSTERING ORDER BY (upload DESC);
     */
    public static void createTableFiles(Session session) {
        SchemaStatement filesStmt = SchemaBuilder
                .createTable(FILES_TABLENAME).ifNotExists()
                .addPartitionKey(FILES_FILENAME, DataType.text())
                .addClusteringColumn(FILES_UPLOAD, DataType.timestamp())
                .addStaticColumn(FILES_EXTENSION, DataType.text())
                .addColumn(FILES_BINARY, DataType.blob())
                .withOptions().clusteringOrder(FILES_UPLOAD, Direction.DESC);
        session.execute(filesStmt);
        LOGGER.info("+ Table '{}' has been created (if needed).", FILES_TABLENAME);        
    }

    /**
     * CREATE TABLE IF NOT EXISTS videos_views (
     *    videoid     uuid,
     *    views       counter,
     *    PRIMARY KEY (videoid)
     * );
     */
    public static void createTableVideoViews(Session session) {
        SchemaStatement videoViewsStmt = SchemaBuilder
                .createTable(VIDEO_VIEWS_TABLENAME).ifNotExists()
                .addPartitionKey(VIDEO_VIEWS_VIDEOID, DataType.uuid())
                .addColumn(VIDEO_VIEWS_VIEWS, DataType.counter());
        session.execute(videoViewsStmt);
        LOGGER.info("+ Table '{}' has been created (if needed).", VIDEO_VIEWS_TABLENAME);
    }
    
    /**
     * CREATE TABLE IF NOT EXISTS comments_by_user (
     *   userid uuid,
     *   commentid timeuuid,
     *   videoid uuid,
     *   comment text,
     *   PRIMARY KEY (userid, commentid)
     * ) WITH CLUSTERING ORDER BY (commentid DESC);
     */
    public static void createTableCommentByUser(Session session) {
        SchemaStatement commentByUserStmt = SchemaBuilder
                .createTable(COMMENT_BY_USER_TABLENAME).ifNotExists().
                 addPartitionKey(COMMENT_BY_USER_USERID, DataType.uuid()).
                 addClusteringColumn(COMMENT_BY_USER_COMMENTID, DataType.timeuuid()).
                 addColumn(COMMENT_BY_USER_VIDEOID, DataType.uuid()).
                 addColumn(COMMENT_BY_USER_COMMENT, DataType.text()).
                 withOptions().
                 clusteringOrder(COMMENT_BY_VIDEO_COMMENTID, Direction.DESC);
        session.execute(commentByUserStmt);
        LOGGER.info("+ Table '{}' has been created (if needed).", COMMENT_BY_USER_TABLENAME);
    }
    
    /**
     * CREATE TABLE IF NOT EXISTS comments_by_video (
     *   videoid uuid,
     *   commentid timeuuid,
     *   userid uuid,
     *   comment text,
     *   PRIMARY KEY (videoid, commentid)
     * ) WITH CLUSTERING ORDER BY (commentid DESC);
     */
    public static void createTableCommentByVideo(Session session) {
        SchemaStatement commentByVideoStmt = SchemaBuilder
                .createTable(COMMENT_BY_VIDEO_TABLENAME).ifNotExists().
                 addPartitionKey(COMMENT_BY_VIDEO_VIDEOID, DataType.uuid()).
                 addClusteringColumn(COMMENT_BY_VIDEO_COMMENTID, DataType.timeuuid()).
                 addColumn(COMMENT_BY_VIDEO_USERID, DataType.uuid()).
                 addColumn(COMMENT_BY_VIDEO_COMMENT, DataType.text()).
                 withOptions().
                 clusteringOrder(COMMENT_BY_VIDEO_COMMENTID, Direction.DESC);
        session.execute(commentByVideoStmt);
        LOGGER.info("+ Table '{}' has been created (if needed).", COMMENT_BY_VIDEO_TABLENAME);
    }
    
    public static void dropKeyspace(Cluster cluster) {
        LOGGER.info("Connected to Cassandra Cluster");
        try(Session session = cluster.connect()) {
            session.execute(SchemaBuilder.dropKeyspace(KEYSPACE_NAME).ifExists());
            LOGGER.info("+ Keyspace '{}' has been dropped (if needed).", KEYSPACE_NAME);
        }
    }
    
    public static void dropKeyspace() {
        try(Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build()) {
            dropKeyspace(cluster);
        }
    }
    
    public static final <T> CompletableFuture<T> asCompletableFuture(final ListenableFuture<T> listenableFuture) {
        //create an instance of CompletableFuture
        CompletableFuture<T> completable = new CompletableFuture<T>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                boolean result = listenableFuture.cancel(mayInterruptIfRunning);
                super.cancel(mayInterruptIfRunning);
                return result;
            }
        };
        // add callback
        Futures.addCallback(listenableFuture, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                completable.complete(result);
            }
            @Override
            public void onFailure(Throwable t) {
                completable.completeExceptionally(t);
            }
        });
        return completable;
    }
}
