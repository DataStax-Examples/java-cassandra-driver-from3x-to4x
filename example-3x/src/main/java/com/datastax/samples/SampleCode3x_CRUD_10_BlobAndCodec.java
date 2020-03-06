package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSessionAndCluster;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableFiles;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.samples.codec.BytesArrayCodec;
import com.datastax.samples.dto.FileDto;

/**
 * Sample codes using Cassandra OSS Driver 3.x
 * 
 * Disclaimers:
 *  - Tests for arguments nullity has been removed for code clarity
 *  - Packaged as a main class for usability
 *  
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 * 
 * CREATE TABLE IF NOT EXISTS files (
 *  filename  text,
 *  upload    timestamp,
 *  extension text static,
 *  binary    blob,
 *  PRIMARY KEY((filename), upload)
 * ) WITH CLUSTERING ORDER BY (upload DESC);
 * 
 * @author Cedrick LUNVEN (@clunven)
 * @author Erick  RAMIREZ (@@flightc)
 */
public class SampleCode3x_CRUD_10_BlobAndCodec implements ExampleSchema {

    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CRUD_10_BlobAndCodec.class);

    // This will be used as singletons for the sample
    private static Cluster cluster;
    private static Session session;
    
    // Prepare your statements once and execute multiple times 
    private static PreparedStatement stmtInsertFile;
    private static PreparedStatement stmtReadFile;
    
    /** StandAlone (vs JUNIT) to help you running.  */
    public static void main(String[] args) throws Exception {
        try {
            
            // === INITIALIZING ===
            
            // Create killrvideo keyspace (if needed)
            createKeyspace();

            // Initialize Cluster and Session Objects (connected to keyspace killrvideo)
            session = connect(cluster);
            
            // Create working table User (if needed)
            createTableFiles(session);
            
            // Empty tables for tests
            truncateTable(session, FILES_TABLENAME);
            
            // Prepare your statements once and execute multiple times 
            prepareStatements();
            
            // ========== CREATE ===========
            
            insertFile("/cassandra_logo.png");
            
            // ========= READ ==============
            
            Optional<FileDto> fileDto = readFileFromDB("cassandra_logo.png");
            if (fileDto.isPresent()) {
                LOGGER.info("+ File {} has been retrieved from Cassandra", fileDto.get().getFilename());
                String newName = "logo2.png";
                saveTempFile(fileDto.get(), newName);
                LOGGER.info("+ File {} has been created in temp", newName);
            }
            
            
           /* If you want to read content as a byte[] directly create a TypeCodec
            * (default data returned is ByteBuffer)
            */
           byte[] fileBinaryContent = readContentAsByteArray("cassandra_logo.png");
           LOGGER.info("+ Length of file {} byte(s)", fileBinaryContent.length);
           
        } finally {
            closeSessionAndCluster(session, cluster);
        }
        System.exit(0);
    }
    
    private static void insertFile(String path) 
    throws IOException {
        File myFile = new File(SampleCode3x_CRUD_10_BlobAndCodec.class.getResource(path).getFile());
        if (!myFile.exists()) {
            throw new IllegalArgumentException("File " + myFile.getAbsolutePath() + " does not exists or cannot be read");
        }
        String[] parts = myFile.getName().split("\\.");
        session.execute(stmtInsertFile.bind()
            .setString(FILES_FILENAME, myFile.getName())
            .setString(FILES_EXTENSION, parts[1])
            .setTimestamp(FILES_UPLOAD, new Date())
            .setBytes(FILES_BINARY, readBytesFromFile(myFile)));
        LOGGER.info("+ Record {} has been created in Cassandra", myFile.getName());
    }
    
    private static Optional<FileDto> readFileFromDB(String filename) {
        ResultSet rs = session.execute(stmtReadFile.bind()
                .setString(FILES_FILENAME, filename));
        if (rs.getAvailableWithoutFetching() == 0) {
            return Optional.empty();
        }
        Row row = rs.one();
        FileDto targetFile = new FileDto();
        targetFile.setFilename(row.getString(FILES_FILENAME));
        targetFile.setUpload(row.getTimestamp(FILES_UPLOAD));
        targetFile.setContent(row.getBytes(FILES_BINARY));
        targetFile.setExtension(row.getString(FILES_EXTENSION));
        return Optional.of(targetFile);
    }
    
    private static byte[] readContentAsByteArray(String filename) {
        Row row = session.execute(QueryBuilder
                         .select().column(FILES_BINARY).from(FILES_TABLENAME)
                         .where(QueryBuilder.eq(FILES_FILENAME, filename))
                         .limit(1)).one();
        return row.get(FILES_BINARY, new BytesArrayCodec());
    }
    
    private static void saveTempFile(FileDto dto, String newfileName) 
    throws IOException {
        File tmpFile = File.createTempFile(newfileName, "." + dto.getExtension());
        LOGGER.info("+ Creating... {}", tmpFile.getAbsolutePath());
        writeBytesToFile(dto.getContent(), tmpFile);
    }
    
    private static void prepareStatements() {
        
        stmtInsertFile = session.prepare(QueryBuilder
                .insertInto(FILES_TABLENAME)
                .value(FILES_FILENAME, QueryBuilder.bindMarker(FILES_FILENAME))
                .value(FILES_EXTENSION, QueryBuilder.bindMarker(FILES_EXTENSION))
                .value(FILES_UPLOAD, QueryBuilder.bindMarker(FILES_UPLOAD))
                .value(FILES_BINARY, QueryBuilder.bindMarker(FILES_BINARY)));

        // Get latest version of a file
        stmtReadFile = session.prepare(QueryBuilder
                .select().from(FILES_TABLENAME)
                .where(QueryBuilder.eq(FILES_FILENAME, QueryBuilder.bindMarker()))
                .limit(1));
    }
    
    public static ByteBuffer readBytesFromFile(File file) throws IOException {
        FileInputStream inputStream = null;
        boolean threw = false;
        try {
            inputStream = new FileInputStream(file);
            FileChannel channel = inputStream.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
            channel.read(buffer);
            buffer.flip();
            return buffer;
        } catch (IOException e) {
            threw = true;
            throw e;
        } finally {
            close(inputStream, threw);
        }
    }

    public static void writeBytesToFile(ByteBuffer buffer, File file) throws IOException {
        FileOutputStream outputStream = null;
        boolean threw = false;
        try {
            outputStream = new FileOutputStream(file);
            FileChannel channel = outputStream.getChannel();
            channel.write(buffer);
        } catch (IOException e) {
            threw = true;
            throw e;
        } finally {
            close(outputStream, threw);
        }
    }
    
    private static void close(Closeable inputStream, boolean threw) throws IOException {
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException e) {
                if (!threw) throw e; // else preserve original exception
            }
        }
    }
  
}
