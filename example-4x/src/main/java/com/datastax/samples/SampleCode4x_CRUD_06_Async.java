package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSession;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableUser;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.samples.dto.UserDto;

/**
 * Sample codes using Cassandra OSS Driver 4.x
 * 
 * Disclaimers:
 *  - Tests for arguments nullity has been removed for code clarity
 *  - Packaged as a main class for usability
 *  
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 * 
 * @author Cedrick LUNVEN (@clunven)
 * @author Erick  RAMIREZ (@@flightc)
 */
public class SampleCode4x_CRUD_06_Async implements ExampleSchema {

    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CRUD_06_Async.class);

    // This will be used as singletons for the sample
    private static CqlSession session;
    
    // Prepare your statements once and execute multiple times 
    private static PreparedStatement stmtCreateUser;
    private static PreparedStatement stmtUpsertUser;
    private static PreparedStatement stmtExistUser;
    private static PreparedStatement stmtDeleteUser;
    private static PreparedStatement stmtFindUser;
    
    /** StandAlone (vs JUNIT) to help you running. 
     * @throws ExecutionException 
     * @throws InterruptedException */
    public static void main(String[] args) 
    throws InterruptedException, ExecutionException {
        try {
            
            // === INITIALIZING ===
            
            // Create killrvideo keyspace (if needed)
            createKeyspace();

            // Initialize Cluster and Session Objects (connected to keyspace killrvideo)
            session = connect();
            
            // Create working table User (if needed)
            createTableUser(session);
            
            // Empty tables for tests
            truncateTable(session, USER_TABLENAME);
            
            // Prepare your statements once and execute multiple times 
            prepareStatements();
            
            // ========== CREATE ===========
            
            String userEmail = "clun@sample.com";
            
            existUserAsync(userEmail)
                .thenAccept(exist -> LOGGER.info("+ '{}' exists ? (expecting false): {}", userEmail, exist))
                .thenCompose(r->createUserAsync(userEmail, "Cedric", "Lunven"))
                .thenCompose(r->existUserAsync(userEmail))
                .thenAccept(exist -> LOGGER.info("+ '{}' exists ? (expecting true): {}", userEmail, exist))
                .toCompletableFuture().get(); // enforce blocking call to have logs.
            
           
            // ========= UPDATE ============
            
            String userEmail2 = "eram@sample.com";
            
            existUserAsync(userEmail2)
                .thenAccept(exist -> LOGGER.info("+ '{}' exists ? (expecting false): {}", userEmail2, exist))
                .thenCompose(r->updateUserAsync(userEmail2,  "Eric", "Ramirez"))
                .thenCompose(r->existUserAsync(userEmail2))
                .thenAccept(exist -> LOGGER.info("+ '{}' exists ? (expecting true): {}", userEmail2, exist))
                .toCompletableFuture().get(); // enforce blocking call to have logs.
            
            // ========= DELETE ============
            
            // Delete an existing user by its email (if email does not exist, no error)
            // Delete an existing user by its email (if email does not exist, no error)
            deleteUserAsync(userEmail2)
                .thenCompose(r->existUserAsync(userEmail2))
                .thenAccept(exist -> LOGGER.info("+ '{}' exists ? (expecting false) {}", userEmail2, exist))
                .get(); // enforce blocking call to have logs.
            
            // ========= READ ==============
            
            findUserByIdAsync("eram@sample.com")
                .thenAccept(erick -> LOGGER.info("+ Retrieved '{}': (expecting Optional.empty) {}", userEmail2, erick))
                .get(); // enforce blocking call to have logs.
        
            findUserByIdAsync("clun@sample.com")
                .thenAccept(erick -> LOGGER.info("+ Retrieved '{}': (expecting result) {}", userEmail2, erick))
                .get(); // enforce blocking call to have logs.
            
            // Read all (first upserts)
            updateUserAsync(userEmail2, "Eric", "Ramirez");
            updateUserAsync(userEmail, "Cedrick", "Lunven");
            List<UserDto > allUsers = session
                    .execute(QueryBuilder.selectFrom(USER_TABLENAME).all().build())
                    .all().stream().map(UserDto::new)
                    .collect(Collectors.toList());
            LOGGER.info("+ Retrieved users count {}", allUsers.size());
            
        } finally {
            closeSession(session);
        }
        System.exit(0);
    }
    
    private static CompletionStage<Boolean> existUserAsync(String email) {
        return session.executeAsync(stmtExistUser.bind(email))
                      .thenApply(ars -> ars.one() != null);
    }
    
    private static CompletableFuture<Void> createUserAsync(String email, String firstname, String lastname) {
        return session.executeAsync(stmtCreateUser.bind(email, firstname, lastname))
                .thenAccept(rs -> {
                        if (!rs.wasApplied()) {
                            throw new IllegalArgumentException("Email '" + email + 
                                    "' already exist in Database. Cannot create new user");
                        }
                        LOGGER.info("+ User {} has been created", email);
                      }).toCompletableFuture();
    }
    
    private static CompletableFuture<Void> updateUserAsync(String email, String firstname, String lastname) {
        return session.executeAsync(stmtUpsertUser.bind(email, firstname, lastname))
                .thenAccept(rs -> LOGGER.info("+ User {} has been updated", email))
                .toCompletableFuture();
    }
    
    private static CompletableFuture<Void> deleteUserAsync(String email) {
        return session.executeAsync(stmtDeleteUser.bind(email))
                .thenAccept(rs -> LOGGER.info("+ User {} has been deleted", email))
                .toCompletableFuture();
    }
    
    private static CompletableFuture< Optional < UserDto > > findUserByIdAsync(String email) {
        return session.executeAsync(stmtFindUser.bind(email))
                      .thenApply(SampleCode4x_CRUD_06_Async::mapUserDtoRow)
                      .toCompletableFuture();
    }
    
    private static Optional < UserDto > mapUserDtoRow(AsyncResultSet asyncRS) {
        Row myRow = asyncRS.one();
        if (myRow == null) {
            return Optional.empty();
        }
        return Optional.of(new UserDto(myRow));
    }
    
    private static void prepareStatements() {
        stmtCreateUser = session.prepare(QueryBuilder.insertInto(USER_TABLENAME)
                .value(USER_EMAIL, QueryBuilder.bindMarker())
                .value(USER_FIRSTNAME, QueryBuilder.bindMarker())
                .value(USER_LASTNAME, QueryBuilder.bindMarker())
                .ifNotExists().build());
        // Using a - SLOW - lightweight transaction to check user existence
        stmtUpsertUser = session.prepare(QueryBuilder.insertInto(USER_TABLENAME)
                .value(USER_EMAIL, QueryBuilder.bindMarker())
                .value(USER_FIRSTNAME, QueryBuilder.bindMarker())
                .value(USER_LASTNAME, QueryBuilder.bindMarker())
                .build());
        stmtExistUser = session.prepare(QueryBuilder
                .selectFrom(USER_TABLENAME).column(USER_EMAIL)
                .whereColumn(USER_EMAIL)
                .isEqualTo(QueryBuilder.bindMarker())
                .build());
        stmtDeleteUser = session.prepare(QueryBuilder
                .deleteFrom(USER_TABLENAME)
                .whereColumn(USER_EMAIL)
                .isEqualTo(QueryBuilder.bindMarker())
                .build());
        stmtFindUser = session.prepare(QueryBuilder
                .selectFrom(USER_TABLENAME).all()
                .whereColumn(USER_EMAIL)
                .isEqualTo(QueryBuilder.bindMarker())
                .build());
       
    }
  
}
