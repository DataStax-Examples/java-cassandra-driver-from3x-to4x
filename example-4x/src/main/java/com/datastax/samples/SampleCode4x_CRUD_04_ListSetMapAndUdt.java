package com.datastax.samples;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static com.datastax.samples.ExampleUtils.closeSession;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableVideo;
import static com.datastax.samples.ExampleUtils.createUdtVideoFormat;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.samples.codec.UdtVideoFormatCodec;
import com.datastax.samples.dto.VideoDto;
import com.datastax.samples.dto.VideoFormatDto;

/**
 * Working on advance types LIST, SET, MAP and UDT.
 * 
 * This is the table we work with:
 *
 * CREATE TABLE IF NOT EXISTS videos (
 *   videoid    uuid,
 *   title      text,
 *   upload    timestamp,
 *   email      text,
 *   url        text,
 *   tags       set <text>,
 *   frames     list<int>,
 *   formats    map <text,frozen<video_format>>,
 *   PRIMARY KEY (videoid)
 * );
 * 
 *  using the simple User Defined Type (UDT) video_format
 *  
 * CREATE TYPE IF NOT EXISTS video_format (
 *   width   int,
 *   height  int
 * );
 * 
 * We want to :
 * - Add a new record with Query Builder
 * - Add a new tag (SET) in existing record
 * - Remove a new tag (SET) in existing record
 * - Add a new tag (SET) in existing record
 * - Remove a new tag (SET) in existing record

 */
public class SampleCode4x_CRUD_04_ListSetMapAndUdt implements ExampleSchema {
	
    /** Logger for the class. */
	private static Logger LOGGER = 
	        LoggerFactory.getLogger(SampleCode4x_CRUD_04_ListSetMapAndUdt.class);
    
	// This will be used as singletons for the sample
    private static CqlSession         session;
    private static UserDefinedType videoFormatUdt;
    
    // Prepare your statements once and execute multiple times 
    private static PreparedStatement stmtCreateVideo;
    private static PreparedStatement stmtReadVideoTags;
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        try {
            
            // Create killrvideo keyspace (if needed)
            createKeyspace();

            // Initialize Cluster and Session Objects 
            session = connect();
            
            // Create table
            createUdtVideoFormat(session);
            createTableVideo(session);
            
            // Empty tables for tests
            truncateTable(session, VIDEO_TABLENAME);
            
            // User define type
            videoFormatUdt = session.getMetadata()
                .getKeyspace(KEYSPACE_NAME)
                .flatMap(ks -> ks.getUserDefinedType(UDT_VIDEO_FORMAT_NAME))
                .orElseThrow(() -> new IllegalArgumentException("Missing UDT definition"));
            
            // Prepare your statements once and execute multiple times 
            prepareStatements();
            
            // ========= CREATE ============
            
            UUID myVideoId = Uuids.random();
            
            // Dto wrapping all data but no object Mapping here, QueryBuilder only
            VideoDto dto = new VideoDto();
            dto.setVideoid(myVideoId);
            dto.setTitle("The World’s Largest Apache Cassandra™ NoSQL Event | DataStax Accelerate 2020");
            dto.setUrl("https://www.youtube.com/watch?v=7afxKEH7t8Q");
            dto.setEmail("clun@sample.com");
            dto.getTags().add("cassandra");
            dto.getFrames().addAll(Arrays.asList(2, 3, 5, 8, 13, 21));
            dto.getTags().add("accelerate");
            dto.getFormats().put("mp4", new VideoFormatDto(640, 480));
            dto.getFormats().put("ogg", new VideoFormatDto(640, 480));
            createVideo(dto);
            
            // Operations on SET (add/remove)
            LOGGER.info("+ Tags before adding 'OK' {}", listTagsOnVideo(myVideoId));
            addTagToVideo(myVideoId,  "OK");
            LOGGER.info("+ Tags after adding 'OK' {}", listTagsOnVideo(myVideoId));
            removeTagFromVideo(myVideoId,  "accelerate");
            LOGGER.info("+ Tags after removing 'accelerate' {}", listTagsOnVideo(myVideoId));
            
            // Operations on MAP (add/remove)
            LOGGER.info("+ Formats before {}", listFormatsOnVideo(myVideoId));
            addFormatToVideo(myVideoId, "hd", new VideoFormatDto(1920, 1080));
            LOGGER.info("+ Formats after adding 'mkv' {}", listFormatsOnVideo(myVideoId));
            removeFormatFromVideo(myVideoId, "ogg");
            LOGGER.info("+ Formats after removing 'ogg' {}", listFormatsOnVideo(myVideoId));
            LOGGER.info("+ Formats after removing 'ogg' {}", listFormatsOnVideoWithCustomCodec(myVideoId));
            
            // Operations on LIST (replaceAll, append, replace one
            LOGGER.info("+ Formats frames before {}", listFramesOnVideo(myVideoId));
            updateAllFrames(myVideoId, Arrays.asList(1,2,3));
            LOGGER.info("+ Formats frames after update all {}", listFramesOnVideo(myVideoId));
            appendFrame(myVideoId, 4);
            LOGGER.info("+ Formats frames after append 4 {}", listFramesOnVideo(myVideoId));
            updateOneFrame(myVideoId, 1, 128);
            LOGGER.info("+ Formats frames after changing idx=1 per 128 {}", listFramesOnVideo(myVideoId));
            
        } finally {
            // Close Cluster and Session 
            closeSession(session);
        }
        System.exit(0);
    }
    
    private static void createVideo(VideoDto dto) {

        MutableCodecRegistry registry = (MutableCodecRegistry) session.getContext().getCodecRegistry();
        registry.register(new UdtVideoFormatCodec(registry.codecFor(videoFormatUdt), VideoFormatDto.class));
        
        session.execute(stmtCreateVideo.bind()
                 .setUuid(VIDEO_VIDEOID, dto.getVideoid())
                 .setString(VIDEO_TITLE, dto.getTitle())
                 .setString(VIDEO_USER_EMAIL, dto.getEmail())
                 .setInstant(VIDEO_UPLOAD, Instant.ofEpochMilli(dto.getUpload()))
                 .setString(VIDEO_URL, dto.getUrl())
                 .setSet(VIDEO_TAGS, dto.getTags(), String.class)
                 .setList(VIDEO_FRAMES, dto.getFrames(), Integer.class)
                 .setMap(VIDEO_FORMAT, dto.getFormats(), String.class, VideoFormatDto.class));
        
        /* USING UDT
        Map<String, UdtValue> formats = new HashMap<>();
        if (null != dto.getFormats()) {
            for (Map.Entry<String, VideoFormatDto> dtoEntry : dto.getFormats().entrySet()) {
                formats.put(dtoEntry.getKey(), videoFormatUdt.newValue()
                        .setInt(UDT_VIDEO_FORMAT_WIDTH,  dtoEntry.getValue().getWidth())
                        .setInt(UDT_VIDEO_FORMAT_HEIGHT, dtoEntry.getValue().getHeight()));
            }
        }
        session.execute(stmtCreateVideo.bind()
                .setUuid(VIDEO_VIDEOID, dto.getVideoid())
                .setString(VIDEO_TITLE, dto.getTitle())
                .setString(VIDEO_USER_EMAIL, dto.getEmail())
                .setInstant(VIDEO_UPLOAD, Instant.ofEpochMilli(dto.getUpload()))
                .setString(VIDEO_URL, dto.getUrl())
                .setSet(VIDEO_TAGS, dto.getTags(), String.class)
                .setList(VIDEO_FRAMES, dto.getFrames(), Integer.class)
                .setMap(VIDEO_FORMAT, formats, String.class, UdtValue.class));
        */
        
        
    }
    
    // SET
    
    private static void addTagToVideo(UUID videoId, String newTag) {
       // Note that this statement is not prepared, not supported for add
       session.execute(QueryBuilder
               .update(VIDEO_TABLENAME)
               .appendSetElement(VIDEO_TAGS, literal(newTag))
               .whereColumn(VIDEO_VIDEOID).isEqualTo(literal(videoId))
               .build());
    }
    
    private static void removeTagFromVideo(UUID videoId, String oldTag) {
        // Note that this statement is not prepared, not supported for add
        session.execute(QueryBuilder
                .update(VIDEO_TABLENAME)
                .removeSetElement(VIDEO_TAGS, literal(oldTag))
                .whereColumn(VIDEO_VIDEOID).isEqualTo(literal(videoId))
                .build());
    }
    
    // LIST
    
    private static void updateAllFrames(UUID videoId, List<Integer> values) {
        session.execute(QueryBuilder.update(VIDEO_TABLENAME)
                .setColumn(VIDEO_FRAMES, literal(values))
                .whereColumn(VIDEO_VIDEOID).isEqualTo(literal(videoId))
                .build());
    }
    
    private static void appendFrame(UUID videoId, Integer lastItem) {
        session.execute(QueryBuilder.update(VIDEO_TABLENAME)
                .appendListElement(VIDEO_FRAMES, literal(lastItem))
                .whereColumn(VIDEO_VIDEOID).isEqualTo(literal(videoId))
                .build());
    }
    
    private static void updateOneFrame(UUID videoId, int idx, Integer item) {
        session.execute(QueryBuilder.update(VIDEO_TABLENAME)
                .setListValue(VIDEO_FRAMES, literal(idx), literal(item))
                .whereColumn(VIDEO_VIDEOID).isEqualTo(literal(videoId))
                .build());
    }
    
    // UDT
    
    @SuppressWarnings("unchecked")
    private static void addFormatToVideo(UUID videoId, String key, VideoFormatDto format) {
        // Note that this statement is not prepared, not supported for add
        //UdtValue udType = videoFormatUdt.newValue()
        //        .setInt(UDT_VIDEO_FORMAT_WIDTH,  format.getWidth())
        //        .setInt(UDT_VIDEO_FORMAT_HEIGHT, format.getHeight());
        session.execute(QueryBuilder.update(VIDEO_TABLENAME)
                 .appendMapEntry(VIDEO_FORMAT, literal(key), literal(format, (TypeCodec<VideoFormatDto>)GenericType.of(VideoFormatDto.class)))
                 .whereColumn(VIDEO_VIDEOID).isEqualTo(literal(videoId))
                 .build());
    }
    
    private static void removeFormatFromVideo(UUID videoId, String key) {
        session.execute(QueryBuilder.update(VIDEO_TABLENAME)
                 .remove(VIDEO_FORMAT, literal(key))
                 .whereColumn(VIDEO_VIDEOID).isEqualTo(literal(videoId))
                 .build());
    }
    
    private static Set < String > listTagsOnVideo(UUID videoid) {
        Row row = session.execute(stmtReadVideoTags.bind(videoid)).one();
        return (null == row)  ? new HashSet<>() : row.getSet(VIDEO_TAGS, String.class);
    }
    
    private static List < Integer > listFramesOnVideo(UUID videoid) {
        Row row = session.execute(stmtReadVideoTags.bind(videoid)).one();
        return (null == row)  ? new ArrayList<>() : row.getList(VIDEO_FRAMES, Integer.class);
    }
    
    private static Map < String, VideoFormatDto > listFormatsOnVideo(UUID videoId) {
        Map < String, VideoFormatDto > mapOfFormats = new HashMap<>();
        Row row = session.execute(stmtReadVideoTags.bind(videoId)).one();
        if (null != row) {
            Map < String, UdtValue> myMap = row.getMap(VIDEO_FORMAT, String.class, UdtValue.class);
            for (Map.Entry<String, UdtValue> entry : myMap.entrySet()) {
                mapOfFormats.put(entry.getKey(), new VideoFormatDto(
                        entry.getValue().getInt(UDT_VIDEO_FORMAT_WIDTH),
                        entry.getValue().getInt(UDT_VIDEO_FORMAT_HEIGHT)));
            }
        }
        return mapOfFormats;
    }

    private static Map < String, VideoFormatDto > listFormatsOnVideoWithCustomCodec(UUID videoId) {
        Map < String, VideoFormatDto > mapOfFormats = new HashMap<>();
        Row row = session.execute(stmtReadVideoTags.bind(videoId)).one();
        if (null != row) {
            return row.getMap(VIDEO_FORMAT, String.class, VideoFormatDto.class);
        }
        return mapOfFormats;
    }
    
    
    private static void prepareStatements() {
        stmtCreateVideo = session.prepare(QueryBuilder.insertInto(VIDEO_TABLENAME)
                .value(VIDEO_VIDEOID,    QueryBuilder.bindMarker(VIDEO_VIDEOID))
                .value(VIDEO_TITLE,      QueryBuilder.bindMarker(VIDEO_TITLE))
                .value(VIDEO_USER_EMAIL, QueryBuilder.bindMarker(VIDEO_USER_EMAIL))
                .value(VIDEO_UPLOAD,     QueryBuilder.bindMarker(VIDEO_UPLOAD))
                .value(VIDEO_URL,        QueryBuilder.bindMarker(VIDEO_URL))
                .value(VIDEO_TAGS,       QueryBuilder.bindMarker(VIDEO_TAGS))
                .value(VIDEO_FRAMES,     QueryBuilder.bindMarker(VIDEO_FRAMES))
                .value(VIDEO_FORMAT,     QueryBuilder.bindMarker(VIDEO_FORMAT))
                .build());
        stmtReadVideoTags = session.prepare(QueryBuilder
                .selectFrom(VIDEO_TABLENAME)
                .column(VIDEO_TAGS).column(VIDEO_FORMAT).column(VIDEO_FRAMES)
                .whereColumn(VIDEO_VIDEOID).isEqualTo(QueryBuilder.bindMarker())
                .build());
    }
    
}
