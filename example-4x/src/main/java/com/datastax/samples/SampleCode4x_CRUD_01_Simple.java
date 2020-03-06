package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSession;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableUser;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
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
public class SampleCode4x_CRUD_01_Simple implements ExampleSchema {

    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CRUD_01_Simple.class);

    // This will be used as singletons for the sample
    private static CqlSession session;
    
    // Prepare your statements once and execute multiple times 
    private static PreparedStatement stmtCreateUser;
    private static PreparedStatement stmtUpsertUser;
    private static PreparedStatement stmtExistUser;
    private static PreparedStatement stmtDeleteUser;
    private static PreparedStatement stmtFindUser;
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
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
            
            if (!existUser(userEmail)) {
                LOGGER.info("+ {} does not exists in table 'user'", userEmail);
            }
            
            createUser(userEmail, "Cedric", "Lunven");
            
            if (existUser(userEmail)) {
                LOGGER.info("+ {}  now exists in table 'user'", userEmail);
            }
            
            // ========= UPDATE ============
            
            String userEmail2 = "eram@sample.com";

            if (!existUser(userEmail2)) {
                LOGGER.info("+ {} does not exists in table 'user'", userEmail2);
            } 
            
            updateUser(userEmail2, "Eric", "Ramirez");
            
            if (existUser(userEmail2)) {
                LOGGER.info("+ {}  now exists in table 'user'", userEmail2);
            }
            
            // ========= DELETE ============
            
            // Delete an existing user by its email (if email does not exist, no error)
            deleteUser(userEmail2);
            if (!existUser(userEmail2)) {
                LOGGER.info("+ {} does not exists in table 'user'", userEmail2);
            } 
            
            // ========= READ ==============
            
            // Will be empty as we have deleted it
            Optional<UserDto> erick = findUserById(userEmail2);
            LOGGER.info("+ Retrieved {}: {}", userEmail2,erick); // Expected Optiona.empty()
            
            // Not null
            Optional<UserDto> cedrick = findUserById(userEmail);
            LOGGER.info("+ Retrieved {}: {}", userEmail, cedrick.get().getEmail()); 
            
            // Read all (first upserts)
            updateUser(userEmail2, "Eric", "Ramirez");
            updateUser(userEmail, "Cedrick", "Lunven");
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
    
    private static boolean existUser(String email) {
        return session.execute(stmtExistUser.bind(email)).getAvailableWithoutFetching() > 0;
    }
    
    private static void createUser(String email, String firstname, String lastname) {
        ResultSet rs = session.execute(stmtCreateUser.bind(email, firstname, lastname));
        if (!rs.wasApplied()) {
            throw new IllegalArgumentException("Email '" + email + "' already exist in Database. Cannot create new user");
        }
        LOGGER.info("+ User {} has been created", email);
    }
    
    private static void updateUser(String email, String firstname, String lastname) {
        session.execute(stmtUpsertUser.bind(email, firstname, lastname));
        LOGGER.info("+ User {} has been updated", email);
    }
    
    private static void deleteUser(String email) {
        session.execute(stmtDeleteUser.bind(email));
        LOGGER.info("+ User {} has been deleted", email);
    }
    
    private static Optional < UserDto > findUserById(String email) {
        ResultSet rs = session.execute(stmtFindUser.bind(email));
        // We query by the primary key ensuring unicity
        Row record = rs.one();
        return (null != record) ? Optional.of(new UserDto(record)) :Optional.empty();
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
