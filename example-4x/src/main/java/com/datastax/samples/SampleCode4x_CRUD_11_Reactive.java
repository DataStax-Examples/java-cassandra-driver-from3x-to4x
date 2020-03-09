package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSession;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableUser;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.samples.dto.UserDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
 * Doc: https://docs.datastax.com/en/developer/java-driver/4.4/manual/core/reactive/
 * @author Cedrick LUNVEN (@clunven)
 * @author Erick  RAMIREZ (@@flightc)
 */
public class SampleCode4x_CRUD_11_Reactive implements ExampleSchema {

    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CRUD_11_Reactive.class);

    // This will be used as singletons for the sample
    private static CqlSession session;
    
    // Prepare your statements once and execute multiple times 
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
            
            // ========== CREATE / UPDATE ===========
            
            String userEmail  = "clun@sample.com";
            String userEmail2 = "ram@sample.com";
            
            // Test existence of user 1 to false and then create user 1
            existUserReactive(userEmail)
                .doOnNext(exist -> LOGGER.info("+ '{}' exists ? (expecting false): {}", userEmail, exist))
                .and(upsertUserReactive(userEmail, "Cedric", "Lunven"))
                .block();
            
            // User 1 now exist and we log (blocking call)
            existUserReactive(userEmail)
                .doOnNext(exist -> LOGGER.info("+ '{}' exists ? (expecting false): {}", userEmail, exist))
                .block();
            
            // Creating user 2 to be deleted
            upsertUserReactive(userEmail2,  "Eric", "Ramirez")
                .doOnNext(exist -> LOGGER.info("+ '{}' exists ? (expecting false): {}", userEmail2, exist))
                .block();
            
            // ========= DELETE ============
            
            deleteUserReactive(userEmail2)
                .doOnNext(exist -> LOGGER.info("+ '{}' exists ? (expecting false) {}", userEmail2, exist))
                .block(); // enforce blocking call to have logs.
            
            // ========= READ ==============
            
            // User 2 has been deleted as such will be empty
            findUserByIdReactive("eram@sample.com")
                .doOnNext(erick -> LOGGER.info("+ Retrieved '{}': (expecting Optional.empty) {}", userEmail2, erick))
                .block(); // enforce blocking call to have logs.
        
            // User 1 is there so we should lie 
            findUserByIdReactive("clun@sample.com")
                .doOnNext(erick -> LOGGER.info("+ Retrieved '{}': (expecting result) {}", userEmail2, erick))
                .block(); // enforce blocking call to have logs.
           
            // creating user 2 again to have 2 records in the tables
            upsertUserReactive(userEmail2,  "Eric", "Ramirez")
                .doOnNext(exist -> LOGGER.info("+ '{}' exists ? (expecting false): {}", userEmail2, exist))
                .block();
            // Listing users
            listAllUserReactive()
                .map(UserDto::getEmail)
                .doOnNext(email -> LOGGER.info("+ '{}' email found", email))
                .blockLast();
            
            Thread.sleep(500);
        } finally {
            closeSession(session);
        }
        System.exit(0);
    }
    
    private static Mono<Boolean> existUserReactive(String email) {
        ReactiveResultSet rrs = session.executeReactive(stmtExistUser.bind(email));
        return Mono.from(rrs).map(rs -> true).defaultIfEmpty(false);
    }
    
    private static Mono<Optional<UserDto>> findUserByIdReactive(String email) {
        return Mono.from(session.executeReactive(stmtFindUser.bind(email)))
                   .doOnNext(row -> LOGGER.info("+ Retrieved '{}': (expecting result) {}", row.getString(USER_EMAIL), email))
                   .map(UserDto::new).map(Optional::of)
                   .defaultIfEmpty(Optional.empty());
    }
    
    private static Mono<Void> upsertUserReactive(String email, String firstname, String lastname) {
        ReactiveResultSet rrs = session.executeReactive(stmtUpsertUser.bind(email, firstname, lastname));
        return Mono.from(rrs).then();
    }
    
    private static Mono<Void> deleteUserReactive(String email) {
        return Mono.from(session.executeReactive(stmtDeleteUser.bind(email))).then();
    }
    
    private static Flux<UserDto> listAllUserReactive() {
        SimpleStatement queryAllUser = QueryBuilder.selectFrom(USER_TABLENAME).all().build();
        return Flux.from(session.executeReactive(queryAllUser)).map(UserDto::new);
    }
    
    private static void prepareStatements() {
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
