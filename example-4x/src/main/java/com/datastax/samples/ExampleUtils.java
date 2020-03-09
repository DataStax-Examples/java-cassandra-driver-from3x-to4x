package com.datastax.samples;

import java.net.InetSocketAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;

/**
 * Code reused in multiple samples.
 *
 */
public class ExampleUtils implements ExampleSchema {

    private static Logger LOGGER = LoggerFactory.getLogger(ExampleUtils.class);
    
    public static CqlSession connect() {
        CqlSession cqlSession =  CqlSession.builder()
            .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
            .withKeyspace(KEYSPACE_NAME)
            .withLocalDatacenter("datacenter1")
            .build();
        LOGGER.info("[OK] Connected to Keyspace {} on node 127.0.0.1", KEYSPACE_NAME);
        return cqlSession;
    }
    
    public static void closeSession(CqlSession session) {
        if (session != null) session.close();
        LOGGER.info("[OK]Session is now closed");
    }
    
    public static void truncateTable(CqlSession session, String tableName) {
        session.execute(QueryBuilder.truncate(tableName).build());
    }
    
    public static void dropTableIfExists(CqlSession session, String tableName) {
        session.execute(SchemaBuilder.dropTable(tableName).ifExists().build());
    }
    
    public static void dropTypeIffExists(CqlSession session, String typeName) {
        session.execute(SchemaBuilder.dropType(typeName).ifExists().build());
    }
    
    /**
     * CREATE KEYSPACE killrvideo 
     * WITH replication = 
     *      {'class': 'SimpleStrategy', 'replication_factor': '1'}  
     * AND durable_writes = true;
     */
    public static SimpleStatement createKeyspaceSimpleStrategy(String keyspaceName, int replicationFactor) {
        return SchemaBuilder.createKeyspace(keyspaceName)
                    .ifNotExists()
                    .withSimpleStrategy(replicationFactor)
                    .withDurableWrites(true)
                    .build();
    }
    
    public static void createKeyspace() {
        try (CqlSession cqlSession = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1")
                .build()) {
            cqlSession.execute(createKeyspaceSimpleStrategy(KEYSPACE_NAME, KEYSPACE_REPLICATION_FACTOR));
            LOGGER.info("+ Keyspace '{}' created (if needed).", KEYSPACE_NAME);
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
    public static void createTableUser(CqlSession session) {
        session.execute(SchemaBuilder.createTable(USER_TABLENAME)
                    .ifNotExists()
                    .withPartitionKey(USER_EMAIL, DataTypes.TEXT)
                    .withColumn(USER_FIRSTNAME, DataTypes.TEXT)
                    .withColumn(USER_LASTNAME, DataTypes.TEXT)
                    .build());
        LOGGER.info("+ Table '{}' has been created (if needed).", USER_TABLENAME);
    }
    
    /**
     * CREATE TYPE IF NOT EXISTS video_format (
     *   width   int,
     *   height  int
     *);
     */
    public static void createUdtVideoFormat(CqlSession session) {
        session.execute(SchemaBuilder
                .createType(UDT_VIDEO_FORMAT_NAME)
                .ifNotExists()
                .withField(UDT_VIDEO_FORMAT_WIDTH, DataTypes.INT)
                .withField(UDT_VIDEO_FORMAT_HEIGHT, DataTypes.INT)
                .build());
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
    public static void createTableVideo(CqlSession session) {
        session.execute(SchemaBuilder
                .createTable(VIDEO_TABLENAME)
                .ifNotExists()
                .withPartitionKey(VIDEO_VIDEOID, DataTypes.UUID)
                .withColumn(VIDEO_TITLE, DataTypes.TEXT)
                .withColumn(VIDEO_UPLOAD, DataTypes.TIMESTAMP)
                .withColumn(VIDEO_USER_EMAIL, DataTypes.TEXT)
                .withColumn(VIDEO_URL, DataTypes.TEXT)
                .withColumn(VIDEO_TAGS, DataTypes.setOf(DataTypes.TEXT))
                .withColumn(VIDEO_FRAMES, DataTypes.listOf(DataTypes.INT))
                .withColumn(VIDEO_FORMAT, DataTypes.mapOf(DataTypes.TEXT, 
                        SchemaBuilder.udt(UDT_VIDEO_FORMAT_NAME, true))).build());
        LOGGER.info("+ Table '{}' has been created (if needed).", VIDEO_TABLENAME);
    }

    /**
     * CREATE TABLE IF NOT EXISTS videos_views (
     *    videoid     uuid,
     *    views       counter,
     *    PRIMARY KEY (videoid)
     * );
     */
    public static void createTableVideoViews(CqlSession session) {
        session.execute(SchemaBuilder
                .createTable(VIDEO_VIEWS_TABLENAME)
                .ifNotExists()
                .withPartitionKey(VIDEO_VIEWS_VIDEOID, DataTypes.UUID)
                .withColumn(VIDEO_VIEWS_VIEWS, DataTypes.COUNTER)
                .build());
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
    public static void createTableCommentByUser(CqlSession session) {
        session.execute(SchemaBuilder
                .createTable(COMMENT_BY_USER_TABLENAME)
                .ifNotExists()
                .withPartitionKey(COMMENT_BY_USER_USERID, DataTypes.UUID)
                .withClusteringColumn(COMMENT_BY_USER_COMMENTID, DataTypes.TIMEUUID)
                .withColumn(COMMENT_BY_USER_VIDEOID, DataTypes.UUID)
                .withColumn(COMMENT_BY_USER_COMMENT, DataTypes.TEXT)
                .withClusteringOrder(COMMENT_BY_USER_COMMENTID, ClusteringOrder.DESC)
                .build());
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
    public static void createTableCommentByVideo(CqlSession session) {
        session.execute(SchemaBuilder
                .createTable(COMMENT_BY_VIDEO_TABLENAME)
                .ifNotExists()
                .withPartitionKey(COMMENT_BY_VIDEO_VIDEOID, DataTypes.UUID)
                .withClusteringColumn(COMMENT_BY_VIDEO_COMMENTID, DataTypes.TIMEUUID)
                .withColumn(COMMENT_BY_VIDEO_USERID, DataTypes.UUID)
                .withColumn(COMMENT_BY_VIDEO_COMMENT, DataTypes.TEXT)
                .withClusteringOrder(COMMENT_BY_VIDEO_COMMENTID, ClusteringOrder.DESC)
                .build());
        LOGGER.info("+ Table '{}' has been created (if needed).", COMMENT_BY_VIDEO_TABLENAME);
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
    public static void createTableFiles(CqlSession session) {
        session.execute(SchemaBuilder
                .createTable(FILES_TABLENAME).ifNotExists()
                .withPartitionKey(FILES_FILENAME, DataTypes.TEXT)
                .withClusteringColumn(FILES_UPLOAD, DataTypes.TIMESTAMP)
                .withStaticColumn(FILES_EXTENSION, DataTypes.TEXT)
                .withColumn(FILES_BINARY, DataTypes.BLOB)
                .withClusteringOrder(FILES_UPLOAD, ClusteringOrder.DESC)
                .build());
        LOGGER.info("+ Table '{}' has been created (if needed).", FILES_TABLENAME);        
    }
    
}
