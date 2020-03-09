package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;


/**
 * CREATE TABLE IF NOT EXISTS users (
 *  email      text,
 *  firstname  text,
 *  lastname   text,
 *  PRIMARY KEY (email)
 * );
 */
public class SampleCode4x_CRUD_09_LightweightTransactions implements ExampleSchema {

    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CRUD_09_LightweightTransactions.class);
    
    // This will be used as singletons for the sample
    private static CqlSession session;
    
    // Prepare your statements once and execute multiple times 
    private static PreparedStatement stmtCreateUser;
    private static PreparedStatement stmtUpdateUserLwt;
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        try {

            // Initialize Cluster and Session Objects 
            session = connect();
            
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
            closeSession(session);
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
                .ifNotExists()
                .build());
        
        /* 
         * UPDATE users SET lastname=:lastname
         * WHERE email=:email
         * IF firstname=:firstname
         * 
         * Operators available for LWT Condition: 
         * =, <, <=, >, >=, != and IN
         */
        stmtUpdateUserLwt = session.prepare(QueryBuilder.update(USER_TABLENAME)
                .setColumn(USER_LASTNAME, QueryBuilder.bindMarker(USER_LASTNAME))
                .whereColumn(USER_EMAIL).isEqualTo(QueryBuilder.bindMarker(USER_EMAIL))
                .ifColumn(USER_FIRSTNAME).isEqualTo(QueryBuilder.bindMarker(USER_FIRSTNAME))
                .build());
    }

}
