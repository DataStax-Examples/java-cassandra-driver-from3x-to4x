package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSessionAndCluster;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableUser;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
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
 * 
 * Need Help ? Join us on community.datastax.com to ask your questions for free.
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
        LOGGER.info("Starting 'Simple' sample...");
        
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
            
            /**
             * Given the table 'users' let's insert data
             *
             * CREATE TABLE IF NOT EXISTS users (
             *  email      text,
             *  firstname  text,
             *  lastname   text,
             *  PRIMARY KEY (email)
             * );
             */
            
            LOGGER.info("Illustrating ways to execute queries:");
            
            /* 
             * 1. You can execute a CQL Statement as a string...
             */
            session.execute(
                    "INSERT INTO users(email, firstname,lastname) "
                    + "VALUES('clun1@sample.com','Cedric', 'Lunven')");
            LOGGER.info("+ 'clun1@sample.com' inserted using CQL string");
            
            /* 
             * 2. ...But to avoid CQL injection and ease object mapping you externalize the parameters
             *    First way to externalize is to use ?
             */
            session.execute(""
                    + "INSERT INTO users(email, firstname,lastname) "
                    + "VALUES(?,?, ?)", "clun2@sample.com", "Cedric", "Lunven");
            LOGGER.info("+ 'clun2@sample.com' inserted using CQL string and externalized params");
            
            /* 
             * 3. Parameters could also have some labels and be provided as MAP.
             */
            Map <String, Object > paramsMap = new HashMap<>();
            paramsMap.put("email", "clun3@sample.com");
            paramsMap.put("firstname", "Cedric");
            paramsMap.put("lastname", "Lunven");
            session.execute(""
                    + "INSERT INTO users(email, firstname,lastname) "
                    + "VALUES(:email, :firstname,:lastname)", paramsMap);
            LOGGER.info("+ 'clun3@sample.com' inserted using CQL string and labelled params");
            
            
            /*
             * 4. Instead of writing the statement you can leverage on a QUERY BUILDER
             */
            session.execute(QueryBuilder.insertInto("users")
                                        .value("email", "clun4@sample.com")
                                        .value("firstname","Cedric")
                                        .value("lastname","Lunven"));
            LOGGER.info("+ 'clun4@sample.com' inserted using query builder simple");
            
            session.execute(QueryBuilder.insertInto("users")
                                        .value("email", QueryBuilder.bindMarker())
                                        .value("firstname",QueryBuilder.bindMarker())
                                        .value("lastname",QueryBuilder.bindMarker()).getQueryString(), 
                                        "clun5@sample.com", "Cedric", "Lunven");
            LOGGER.info("+ 'clun5@sample.com' inserted using query builder positions");
            
            // Best practice to group all constants to dedicated class
            session.execute(QueryBuilder.insertInto(USER_TABLENAME)
                    .value(USER_EMAIL, QueryBuilder.bindMarker(USER_EMAIL))
                    .value(USER_FIRSTNAME, QueryBuilder.bindMarker(USER_EMAIL))
                    .value(USER_LASTNAME, QueryBuilder.bindMarker(USER_EMAIL)).getQueryString(), 
                    paramsMap);
            LOGGER.info("+ 'clun6@sample.com' inserted using query builder labels");
            
            
            /*
             * 5. To speed your queries and validate syntaxt only once you should prepare your statements
             */
            PreparedStatement ps = session.prepare(QueryBuilder.insertInto(USER_TABLENAME)
                    .value(USER_EMAIL, QueryBuilder.bindMarker())
                    .value(USER_FIRSTNAME, QueryBuilder.bindMarker())
                    .value(USER_LASTNAME, QueryBuilder.bindMarker()));
            // Then you bind the parameter
            BoundStatement bs = ps.bind("clun6@sample.com", "Cedric", "Lunven");
            session.execute(bs);
            LOGGER.info("+ 'clun7@sample.com' inserted proper way (prepare + query builder)");
            
            
            // ==============================================================================
            // === FROM NOW ON ALL QUERIES WILL USE QUERY BUILDER AND PREPARED STATEMENTS ===
            // ==============================================================================
            
            LOGGER.info("Illustratting Create, Read, Update, Delete:");
            
            LOGGER.info("Create");
            String userEmail = "clun@sample.com";
            createUser(userEmail, "Cedric", "Lunven");
            if (existUser(userEmail)) {
                LOGGER.info("+ {}  now exists in table 'user'", userEmail);
            }
            
            // ========= UPDATE ============
            
            LOGGER.info("Upsert");
            String userEmail2 = "eram@sample.com";

            if (!existUser(userEmail2)) {
                LOGGER.info("+ {} does not exists in table 'user'", userEmail2);
            } 
            
            updateUser(userEmail2, "Eric", "Ramirez");
            
            if (existUser(userEmail2)) {
                LOGGER.info("+ {}  now exists in table 'user'", userEmail2);
            }
            
            // ========= DELETE ============
            
            LOGGER.info("Delete");
            // Delete an existing user by its email (if email does not exist, no error)
            deleteUser(userEmail2);
            if (!existUser(userEmail2)) {
                LOGGER.info("+ {} does not exists in table 'user'", userEmail2);
            } 
            
            // ========= READ ==============
            
            LOGGER.info("Read");
            
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
        /**
         * 2 ways to create a statement
         * INSERT INTO users(email, firstname,lastname) VALUES(?,?,?)
         * INSERT INTO users(email, firstname,lastname) VALUES(:email,:firstname,:lastname)
         */
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
