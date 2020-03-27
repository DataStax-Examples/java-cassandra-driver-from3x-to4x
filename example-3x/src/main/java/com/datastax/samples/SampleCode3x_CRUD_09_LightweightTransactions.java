package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSessionAndCluster;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createTableUser;
import static com.datastax.samples.ExampleUtils.truncateTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Show how to execute LWT (IF NOT EXISTS, nother IF) and parse wasApplied.
 *  
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 * 
 * @author Cedrick LUNVEN (@clunven)
 * @author Erick  RAMIREZ (@@flightc)
 * 
 * Need Help ? Join us on community.datastax.com to ask your questions for free.
 */
public class SampleCode3x_CRUD_09_LightweightTransactions implements ExampleSchema {

    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CRUD_09_LightweightTransactions.class);
    
    // This will be used as singletons for the sample
    private static Cluster cluster;
    private static Session session;
    
    // Prepare your statements once and execute multiple times 
    private static PreparedStatement stmtCreateUser;
    private static PreparedStatement stmtUpdateUserLwt;
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        try {

            // Initialize Cluster and Session Objects 
            session = connect(cluster);
            
            // Use PreparedStatement for queries that are executed multiple times in your application
            prepareStatements();
            
            // Create working table User (if needed)
            createTableUser(session);
            
            // Empty tables for tests
            truncateTable(session, USER_TABLENAME);
            
            // Insert if not exist
            boolean first  = createUserIfNotExist("clun@sample.com", "Cedric", "Lunven");
            boolean second = createUserIfNotExist("clun@sample.com", "Cedric", "Lunven");
            LOGGER.info("+ Created first time ? {} and second time {}", first, second);
            
            // Update if condition
            boolean applied1 = updateIf("clun@sample.com", "Cedric", "BEST");
            boolean applied2 = updateIf("clun@sample.com", "Cedrick", "Lunven");
            LOGGER.info("+ Applied when correct value ? {} and invalid value {}", applied1, applied2);
            
            
        } finally {
            closeSessionAndCluster(session, cluster);
        }
        System.exit(0);
    }
    
    /**
     * The resultset is applied only if the record is created. If not the resultSet is populated
     * with existing data in DB (read)
     */
    private static boolean createUserIfNotExist(String email, String firstname, String lastname) {
        return session.execute(stmtCreateUser.bind(email, firstname, lastname)).wasApplied();
    }
    
    /**
     * Note: we named the parameters as they are not in the same order in the query.
     */
    private static boolean updateIf(String email, String expectedFirstName, String newLastName) {
        return session.execute(stmtUpdateUserLwt.bind()
                .setString(USER_EMAIL, email)
                .setString(USER_FIRSTNAME, expectedFirstName)
                .setString(USER_LASTNAME, newLastName)).wasApplied();
    }
    
    /**
     * Documentation
     * https://docs.datastax.com/en/developer/java-driver/3.8/manual/statements/prepared/#prepared-statements
     */
    private static void prepareStatements() {
        
        /* 
         * INSERT INTO users (email, firstname, lastname)
         * VALUES(?,?,?)
         * IF NOT EXISTS
         */
        stmtCreateUser = session.prepare(QueryBuilder.insertInto(USER_TABLENAME)
                .value(USER_EMAIL, QueryBuilder.bindMarker())
                .value(USER_FIRSTNAME, QueryBuilder.bindMarker())
                .value(USER_LASTNAME, QueryBuilder.bindMarker())
                .ifNotExists());
        
        /* 
         * UPDATE users SET lastname=:lastname
         * WHERE email=:email
         * IF firstname=:firstname
         * 
         * Operators available for LWT Condition: 
         * =, <, <=, >, >=, != and IN
         */
        stmtUpdateUserLwt = session.prepare(QueryBuilder.update(USER_TABLENAME)
                .with(QueryBuilder.set(USER_LASTNAME, QueryBuilder.bindMarker(USER_LASTNAME)))
                .where(QueryBuilder.eq(USER_EMAIL, QueryBuilder.bindMarker(USER_EMAIL)))
                .onlyIf(QueryBuilder.eq(USER_FIRSTNAME, QueryBuilder.bindMarker(USER_FIRSTNAME))));
    }

}
