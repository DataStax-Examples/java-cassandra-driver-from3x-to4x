package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSessionAndCluster;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableVideo;
import static com.datastax.samples.ExampleUtils.createUdtVideoFormat;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.extras.codecs.json.JacksonJsonCodec;
import com.datastax.samples.dto.VideoDto;

public class SampleCode3x_CRUD_05_Json implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CRUD_04_ListSetMapAndUdt.class);
    
    // This will be used as singletons for the sample
    private static Cluster cluster;
    private static Session session;
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        try {

            // Create killrvideo keyspace (if needed)
            createKeyspace();
            
            // Initialize Cluster and Session Objects
            session = connect(cluster);
            
            // Create required tables
            createUdtVideoFormat(session);
            createTableVideo(session);
            
            // Empty tables for tests
            truncateTable(session, VIDEO_TABLENAME);
            
            // Insert as a String - with regular Core CQL
            UUID videoid1 = UUIDs.random();
            session.execute(""
                    + "INSERT INTO " + VIDEO_TABLENAME + "(videoid, email, title, upload, url, tags, frames, formats) "
                    + "VALUES("+ videoid1.toString() +", "
                    + "  'clu@sample.com', 'sample video',"
                    + "  toTimeStamp(now()), 'http://google.fr', "
                    + "  { 'cassandra','accelerate','2020'}, "
                    + "  [ 1, 2, 3, 4], "
                    + "  { 'mp4':{width:1,height:1}, "
                    + "    'ogg':{width:1,height:1} "
                    + "  });");
            LOGGER.info("+ Video 'e7ae5cf3-d358-4d99-b900-85902fda9bb0' has been inserted");
            
            // Insert as a String - with a Column as JSON
            UUID videoid2 = UUIDs.random();
            session.execute("INSERT INTO " + VIDEO_TABLENAME + "(videoid, email, title, upload, url, tags, frames, formats) "
                          + "VALUES(?,?,?,?,?,?,?,fromJson(?))", 
                          videoid2, "clu@sample.com", "sample video",  new Date(),
                          "http://google.fr",  new HashSet<>(), Arrays.asList(1,2,3,4), 
                          "{ \"mp4\":{\"width\":1,\"height\":1}, \"ogg\":{\"width\":1,\"height\":1} }");
            LOGGER.info("+ Video '{}' has been inserted", videoid2);
            
            // Insert as a JSON String
            UUID videoid3 = UUIDs.random();
            session.execute(""
                    + "INSERT INTO " + VIDEO_TABLENAME + " JSON '{"
                    + "\"videoid\":\""+videoid3.toString()+"\"," 
                    + "\"email\":\"clu@sample.com\"," 
                    + "\"title\":\"sample video\"," 
                    + "\"upload\":\"2020-02-26 15:09:22 +00:00\"," 
                    + "\"url\":\"http://google.fr\","
                    + "\"frames\": [1,2,3,4],"
                    + "\"tags\": [\"cassandra\",\"accelerate\", \"2020\"],"
                    + "\"formats\": {" 
                    + "   \"mp4\":{\"width\":1,\"height\":1},"
                    + "   \"ogg\":{\"width\":1,\"height\":1}"
                    + "}}'");
            LOGGER.info("+ Video '{}' has been inserted", videoid3);
            
            // Insert as a JSON Param
            UUID videoid4 = UUIDs.random();
            session.execute(""
                    + "INSERT INTO " + VIDEO_TABLENAME + " JSON ? ", "{"
                    + "\"videoid\":\""+ videoid4.toString() + "\"," 
                    + "\"email\":\"clu@sample.com\"," 
                    + "\"title\":\"sample video\"," 
                    + "\"upload\":\"2020-02-26 15:09:22 +00:00\"," 
                    + "\"url\":\"http://google.fr\","
                    + "\"frames\": [1,2,3,4],"
                    + "\"tags\": [\"cassandra\",\"accelerate\", \"2020\"],"
                    + "\"formats\": {" 
                    + "   \"mp4\":{\"width\":1,\"height\":1},"
                    + "   \"ogg\":{\"width\":1,\"height\":1}"
                    + "}}");
            LOGGER.info("+ Video '{}' has been inserted", videoid4);
            
            // Insert with QueryBuilder - as a JSON String
            UUID videoid5 = UUIDs.random();
            Insert stmt = QueryBuilder.insertInto(VIDEO_TABLENAME).json("{"
                            + "\"videoid\":\""+ videoid5.toString() + "\"," 
                            + "\"email\":\"clu@sample.com\"," 
                            + "\"title\":\"sample video\"," 
                            + "\"upload\":\"2020-02-26 15:09:22 +00:00\"," 
                            + "\"url\":\"http://google.fr\","
                            + "\"frames\": [1,2,3,4],"
                            + "\"tags\": [\"cassandra\",\"accelerate\", \"2020\"],"
                            + "\"formats\": {" 
                            + "   \"mp4\":{\"width\":1,\"height\":1},"
                            + "   \"ogg\":{\"width\":1,\"height\":1}"
                            + "}}");
            session.execute(stmt);
            LOGGER.info("+ Video '{}' has been inserted", videoid5);
            LOGGER.info("[OK] - All video Inserted");
            
            // Insert with QueryBuilder - As an object + Jackson Codec
            UUID videoid6 = UUIDs.random();
            VideoDto dto  = new VideoDto(videoid6, "sample video", "clu@sample.com", "http://google.fr");
            session.execute(QueryBuilder.insertInto(VIDEO_TABLENAME).json(dto));
            LOGGER.info("+ Video '{}' has been inserted", videoid5);
            
            // Read with QueryBuilder - As an object + Jackson Codec
            ResultSet rows = session.execute(QueryBuilder.select().json().from(VIDEO_TABLENAME));
            for (Row row : rows) {
                VideoDto myVideo = row.get(0, VideoDto.class);
                LOGGER.info("+ Video '{}' has been read ", myVideo.getVideoid());
            }
            LOGGER.info("[OK] - All video read");
            
        } finally {
            closeSessionAndCluster(session, cluster);
        }
        System.exit(0);
        
    }
    
    public static Session connect(Cluster cluster) {
        cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withCodecRegistry(
                  new CodecRegistry().register(new JacksonJsonCodec<VideoDto>(VideoDto.class)))
                .build();
        LOGGER.info("Connected to Cluster, Looking for keyspace '{}'...", KEYSPACE_NAME);
        session = cluster.connect(KEYSPACE_NAME);
        LOGGER.info("[OK] Connected to Keyspace");
        return session;
    }
}
