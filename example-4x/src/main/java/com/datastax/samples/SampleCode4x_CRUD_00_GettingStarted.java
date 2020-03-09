package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSession;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableUser;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;

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
public class SampleCode4x_CRUD_00_GettingStarted implements ExampleSchema {

    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CRUD_00_GettingStarted.class);

    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        CqlSession session = null;
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
            
            /** 
             * In this class we will focus only on INSERTING records in order  
             * to detailled all the ways to interact with Cassandra.
             **/

            // #1.a You can execute CQL queries as a String
            session.execute(""
                    + "INSERT INTO users (email, firstname, lastname) "
                    + "VALUES ('clun@sample.com', 'Cedrick', 'Lunven')");
            LOGGER.info("+ Insert as a String");
            
            // #1.b But everything is a statement
            session.execute(SimpleStatement.newInstance(
                      "INSERT INTO users (email, firstname, lastname) "
                    + "VALUES ('clun2@sample.com', 'Cedrick', 'Lunven')"));
            LOGGER.info("+ Insert as a Statement");
            
            // #2.a You should externalize variables using character '?'
            
            // -- option1: add one parameter at a time
            session.execute(SimpleStatement
                           .builder("INSERT INTO users (email, firstname, lastname) VALUES (?,?,?)")
                           .addPositionalValue("clun3@gmail.com")
                           .addPositionalValue("Cedrick")
                           .addPositionalValue("Lunven").build());
            LOGGER.info("+ Insert and externalize var with ?, option1");

            // -- option2: add all parameters in one go
            session.execute(SimpleStatement
                    .builder("INSERT INTO users (email, firstname, lastname) VALUES (?,?,?)")
                    .addPositionalValues("clun4@gmail.com", "Cedrick", "Lunven").build());
            LOGGER.info("+ Insert and externalize var with ?, option2");
            
            // #2.b You can also externalize variables setting a label like :name
          
            // -- option1: add one parameter at a time
            session.execute(SimpleStatement
                    .builder("INSERT INTO users (email, firstname, lastname) VALUES (:e,:f,:l)")
                    .addNamedValue("e", "clun5@gmail.com")
                    .addNamedValue("f", "Cedrick")
                    .addNamedValue("l", "Lunven").build());
            LOGGER.info("+ Insert and externalize var with :labels, option1");
            
            // -- option2: add all parameters in one go
            session.execute(SimpleStatement
                    .builder("INSERT INTO users (email, firstname, lastname) VALUES (:e,:f,:l)")
                    // You can override attributes in the statements
                    .setConsistencyLevel(ConsistencyLevel.ONE)
                    .setTimeout(Duration.ofSeconds(2))
                    .build()
                    .setNamedValues(ImmutableMap.<String, Object>of(
                                                "e", "clun6@gmail.com",
                                                "f", "Cedrick", 
                                                "l", "Lunven")));
            LOGGER.info("+ Insert and externalize var with :labels, option2");
                    
            
            // #4. You can use QueryBuilder to help you building your statements
            session.execute(QueryBuilder
                    .insertInto(USER_TABLENAME)
                    .value(USER_EMAIL, QueryBuilder.literal("clun5@gmail.com"))
                    .value(USER_FIRSTNAME, QueryBuilder.literal("Cedrick"))
                    .value(USER_LASTNAME, QueryBuilder.literal("Lunven"))
                    .build());
            LOGGER.info("+ Insert with QueryBuilder");
            
            // #5.It is recommended to prepare your statements
            // Prepare once, execute multiple times is much faster
            // Use session.prepare(<your_query>)
            
            // 5.a Use '?' for parameters
            // doc: https://docs.datastax.com/en/developer/java-driver/4.5/manual/core/statements/prepared/
            PreparedStatement ps1 = session.prepare("INSERT INTO users (email, firstname, lastname) "
                                                  + "VALUES (?,?,?)");
            BoundStatement bs1 = ps1.bind("clun6@gmail.com", "Cedrick", "Lunven");
            session.execute(bs1);
            LOGGER.info("+ Insert with PrepareStatements");
            
           
            // 5.b To prepare statements with QueryBuilder, use 'bindMarker'
            PreparedStatement ps2 = session.prepare(QueryBuilder
                    .insertInto(USER_TABLENAME)
                    .value(USER_EMAIL, QueryBuilder.bindMarker())
                    .value(USER_FIRSTNAME, QueryBuilder.bindMarker())
                    .value(USER_LASTNAME, QueryBuilder.bindMarker())
                    .build());
            session.execute(ps2.bind("clun7@gmail.com", "Cedrick", "Lunven"));
            LOGGER.info("+ Insert with PrepareStatements + QueryBuilder");
            
            
            // In next SAMPLES you will find everything with QueryBuidler and PreparedStatement
            // Enjoy !!!
            
        } finally {
            closeSession(session);
        }
        System.exit(0);
    }
    
   
  
}
