package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSessionAndCluster;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableUser;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.samples.dto.UserDto;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

/**
 * Show how execute statements asynchronously and parse results.
 *  
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 * 
 * @author Cedrick LUNVEN (@clunven)
 * @author Erick  RAMIREZ (@@flightc)
 * 
 * Need Help ? Join us on community.datastax.com to ask your questions for free.
 */
public class SampleCode3x_CRUD_06_Async implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CRUD_01_Simple.class);

    // This will be used as singletons for the sample
    private static Cluster cluster;
    private static Session session;
    
    // Prepare your statements once and execute multiple times 
    private static PreparedStatement stmtCreateUser;
    private static PreparedStatement stmtUpsertUser;
    private static PreparedStatement stmtExistUser;
    private static PreparedStatement stmtDeleteUser;
    private static PreparedStatement stmtFindUser;
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        
        try {

            // === INITIALIZING ===
            
            // Create killrvideo keyspace (if needed)
            createKeyspace();

            // Initialize Cluster and Session Objects (connected to keyspace killrvideo)
            session = connect(cluster);
            
            // Create working table User (if needed)
            createTableUser(session);
            
            // Empty tables for tests
            truncateTable(session, USER_TABLENAME);
            
            // Prepare your statements once and execute multiple times 
            prepareStatements();
            
            // ========= CREATE ============
            existUserAsync("clun@sample.com")
                .thenAccept(exist -> LOGGER.info("+ 'clun@sample.com' exists ? (expecting false): {}", exist))
                .thenCompose(r->createUserAsync("clun@sample.com", "Cedric", "Lunven"))
                .thenCompose(r->existUserAsync("clun@sample.com"))
                .thenAccept(exist -> LOGGER.info("+ 'clun@sample.com' exists ? (expecting true): {}", exist))
                .get(); // enforce blocking call to have logs.
            
            // ========= UPDATE ============
            existUserAsync("eram@sample.com")
                .thenAccept(exist -> LOGGER.info("+ 'eram@sample.com' exists ? (expecting false) {}", exist))
                .thenCompose(r->updateUserAsync("eram@sample.com", "Eric", "Ramirez"))
                .thenCompose(r->existUserAsync("eram@sample.com"))
                .thenAccept(exist -> LOGGER.info("+ Does 'eram@sample.com' exists ? (expecting true) {}", exist))
                .get(); // enforce blocking call to have logs.
           
            // ========= DELETE ============
            
            // Delete an existing user by its email (if email does not exist, no error)
            deleteUserAsync("eram@sample.com")
                .thenCompose(r->existUserAsync("eram@sample.com"))
                .thenAccept(exist -> LOGGER.info("+ 'eram@sample.com' exists ? (expecting false) {}", exist))
                .get(); // enforce blocking call to have logs.
            
            // ========= READ ==============
            
            findUserByIdAsync("eram@sample.com")
                .thenAccept(erick -> LOGGER.info("+ Retrieved 'eram@sample.com': (expecting Optional.empty) {}", erick))
                .get(); // enforce blocking call to have logs.
            
            findUserByIdAsync("clun@sample.com")
                .thenAccept(erick -> LOGGER.info("+ Retrieved 'clun@sample.com': (expecting result) {}", erick))
                .get(); // enforce blocking call to have logs.
            
        } finally {
            closeSessionAndCluster(session, cluster);
        }
        System.exit(0);
    }
    
    private static CompletableFuture < Boolean > existUserAsync(String email) {
        CompletableFuture<Boolean> cfv = new CompletableFuture<>();
        Futures.addCallback(session.executeAsync(stmtExistUser.bind(email)), new FutureCallback<ResultSet>() {
            // Propagation exception to handle it in the EXPOSITION LAYER. 
            public void onFailure(Throwable ex) { cfv.completeExceptionally(ex); }
            // When resultset has been retrieved we can count record and evaluate existence
            public void onSuccess(ResultSet rs) { cfv.complete(rs.getAvailableWithoutFetching()> 0); }
        });
        return cfv;
    }
    
    private static CompletableFuture<Void> createUserAsync(String email, String firstname, String lastname) {
        CompletableFuture<Void> cfv = new CompletableFuture<>();
        Futures.addCallback(session.executeAsync(stmtCreateUser.bind(email, firstname, lastname)), new FutureCallback<ResultSet>() {
            // Propagation exception to handle it in the EXPOSITION LAYER. 
            public void onFailure(Throwable ex) { cfv.completeExceptionally(ex); }
            // Insertion return Void and we can put null in the complete
            public void onSuccess(ResultSet rs) {
                if (!rs.wasApplied()) {
                    throw new IllegalArgumentException("Email '" + email + "' already exist in Database. Cannot create new user");
                }
                LOGGER.info("+ User {} has been created", email);
                cfv.complete(null);
            }
        });
        return cfv;
    }
    
    private static CompletableFuture<Void> updateUserAsync(String email, String firstname, String lastname) {
        CompletableFuture<Void> cfv = new CompletableFuture<>();
        Futures.addCallback(session.executeAsync(stmtUpsertUser.bind(email, firstname, lastname)), new FutureCallback<ResultSet>() {
            public void onFailure(Throwable ex) { cfv.completeExceptionally(ex); }
            public void onSuccess(ResultSet rs) {
                LOGGER.info("+ User {} has been updated", email);
                cfv.complete(null);
            }
        });
        return cfv;
    }
    
    private static CompletableFuture<Void> deleteUserAsync(String email) {
        CompletableFuture<Void> cfv = new CompletableFuture<>();
        Futures.addCallback(session.executeAsync(stmtDeleteUser.bind(email)), new FutureCallback<ResultSet>() {
            public void onFailure(Throwable ex) { cfv.completeExceptionally(ex); }
            public void onSuccess(ResultSet rs) {
                LOGGER.info("+ User {} has been deleted", email);
                cfv.complete(null);
            }
        });
        return cfv;
    }
    
    private static CompletableFuture< Optional < UserDto > > findUserByIdAsync(String email) {
        CompletableFuture< Optional < UserDto > > cfv = new CompletableFuture<>();
        Futures.addCallback(session.executeAsync(stmtFindUser.bind(email)), new FutureCallback<ResultSet>() {
            public void onFailure(Throwable ex) { cfv.completeExceptionally(ex); }
            public void onSuccess(ResultSet rs) {
                Row record = rs.one();
                if (record == null) {
                    cfv.complete(Optional.empty());
                } else {
                    cfv.complete(Optional.of(new UserDto(record)));
                }
            }
        });
        return cfv;
    }
    
    private static void prepareStatements() {
        stmtCreateUser = session.prepare(QueryBuilder.insertInto(USER_TABLENAME)
                .value(USER_EMAIL, QueryBuilder.bindMarker())
                .value(USER_FIRSTNAME, QueryBuilder.bindMarker())
                .value(USER_LASTNAME, QueryBuilder.bindMarker())
                .ifNotExists());
        // Using a - SLOW - lightweight transaction to check user existence
        stmtUpsertUser = session.prepare(QueryBuilder.insertInto(USER_TABLENAME)
                .value(USER_EMAIL, QueryBuilder.bindMarker())
                .value(USER_FIRSTNAME, QueryBuilder.bindMarker())
                .value(USER_LASTNAME, QueryBuilder.bindMarker()));
        stmtExistUser = session.prepare(QueryBuilder
                .select(USER_EMAIL)
                .from(USER_TABLENAME)
                .where(QueryBuilder.eq(USER_EMAIL, QueryBuilder.bindMarker())));
        stmtDeleteUser = session.prepare(QueryBuilder
                .delete().from(USER_TABLENAME)
                .where(QueryBuilder.eq(USER_EMAIL, QueryBuilder.bindMarker())));
        stmtFindUser = session.prepare(QueryBuilder
                .select().from(USER_TABLENAME)
                .where(QueryBuilder.eq(USER_EMAIL, QueryBuilder.bindMarker())));
       
    }

}
