package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.*;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.samples.dto.UserDto;

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
 * @author Cedrick LUNVEN (@clunven)
 * @author Erick  RAMIREZ (@@flightc)
 */
public class SampleCode3x_CRUD_01_Simple implements ExampleSchema {

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
    public static void main(String[] args) {
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
                    .execute(QueryBuilder.select().from(USER_TABLENAME))
                    .all().stream().map(UserDto::new)
                    .collect(Collectors.toList());
            LOGGER.info("+ Retrieved users count {}", allUsers.size());
            
        } finally {
            closeSessionAndCluster(session, cluster);
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
