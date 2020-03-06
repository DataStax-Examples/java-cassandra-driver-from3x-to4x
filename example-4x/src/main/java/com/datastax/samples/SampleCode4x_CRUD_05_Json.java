package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSession;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableVideo;
import static com.datastax.samples.ExampleUtils.createUdtVideoFormat;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.samples.codec.JsonJacksonTypeCodec;
import com.datastax.samples.dto.VideoDto;

public class SampleCode4x_CRUD_05_Json implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CRUD_05_Json.class);
    
    // This will be used as singletons for the sample
    private static CqlSession session;
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        try {

            // Create killrvideo keyspace (if needed)
            createKeyspace();
            
            // Initialize Cluster and Session Objects
            session = connect();
            
            // Create required tables
            createUdtVideoFormat(session);
            createTableVideo(session);
            
            // Empty tables for tests
            truncateTable(session, VIDEO_TABLENAME);
            
            // Insert as a String - with regular Core CQL
            UUID videoid1 = UUID.randomUUID();
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
            UUID videoid2 = UUID.randomUUID();
            session.execute(SimpleStatement.builder(
                      "INSERT INTO " + VIDEO_TABLENAME + "(videoid, email, title, upload, url, tags, frames, formats) "
                    + "VALUES(?,?,?,?,?,?,?,fromJson(?))")
                    .addPositionalValue(videoid2)
                    .addPositionalValue("clu@sample.com")
                    .addPositionalValue("sample video")
                    .addPositionalValue(Instant.now())
                    .addPositionalValue("http://google.fr")
                    .addPositionalValue(new HashSet<>())
                    .addPositionalValue(Arrays.asList(1,2,3,4))
                    .addPositionalValue("{ \"mp4\":{\"width\":1,\"height\":1}, \"ogg\":{\"width\":1,\"height\":1} }")
                    .build());
            LOGGER.info("+ Video '{}' has been inserted", videoid2);
            
            // Insert as a JSON String
            UUID videoid3 = UUID.randomUUID();
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
            UUID videoid4 = UUID.randomUUID();
            session.execute(SimpleStatement.builder("INSERT INTO " + VIDEO_TABLENAME + " JSON ? ")
                    .addPositionalValue("{"
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
                        + "}}")
                    .build());
            LOGGER.info("+ Video '{}' has been inserted", videoid4);
            
            // Insert with QueryBuilder - as a JSON String
            UUID videoid5 = UUID.randomUUID();
            session.execute(QueryBuilder.insertInto(VIDEO_TABLENAME)
                    .json("{"
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
                        + "}}")
                    .build());
            LOGGER.info("+ Video '{}' has been inserted", videoid5);
            LOGGER.info("[OK] - All video Inserted");
            
            /** 
             * Register a Codec at runtime
             * Can also be achieved at session init with 
             * .addTypeCodecs(new JsonJacksonTypeCodec<VideoDto>(VideoDto.class))
             */
            MutableCodecRegistry registry = (MutableCodecRegistry) session.getContext().getCodecRegistry();
            TypeCodec<VideoDto> jsonCodec = new JsonJacksonTypeCodec<VideoDto>(VideoDto.class);
            registry.register(jsonCodec);
            
            // Insert with QueryBuilder - As an object + Jackson Codec
            UUID videoid6 = UUID.randomUUID();
            VideoDto dto  = new VideoDto(videoid6, "sample video", "clu@sample.com", "http://google.fr");
            session.execute(QueryBuilder
                    .insertInto(VIDEO_TABLENAME)
                    .json(dto, jsonCodec).build());
            LOGGER.info("+ Video '{}' has been inserted", videoid5);
            
            // Read with QueryBuilder - As an object + Jackson Codec
            ResultSet rows = session.execute(QueryBuilder.selectFrom(VIDEO_TABLENAME).json().all().build());
            for (Row row : rows) {
                VideoDto myVideo = row.get(0, VideoDto.class);
                LOGGER.info("+ Video '{}' has been read ", myVideo.getVideoid());
            }
            LOGGER.info("[OK] - All video read");
            
        } finally {
            closeSession(session);
        }
        System.exit(0);
        
    }
}
