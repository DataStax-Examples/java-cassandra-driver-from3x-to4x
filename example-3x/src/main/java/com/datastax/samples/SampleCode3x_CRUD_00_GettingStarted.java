package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSessionAndCluster;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableUser;
import static com.datastax.samples.ExampleUtils.truncateTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.collect.ImmutableMap;

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
public class SampleCode3x_CRUD_00_GettingStarted implements ExampleSchema {

    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CRUD_00_GettingStarted.class);

    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        Cluster cluster = null;
        Session session = null;
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
            
            /** 
             * In this class we will focus only on INSERTING records in order  
             * to detailled all the ways to interact with Cassandra.
             **/

            // #1. You can execute CQL queries as a String
            session.execute(""
                    + "INSERT INTO users (email, firstname, lastname) "
                    + "VALUES ('clun@sample.com', 'Cedrick', 'Lunven')");
            LOGGER.info("+ Insert as a String");
            
            // #2.a You should externalize variables using character '?'
            session.execute(""
                    + "INSERT INTO users (email, firstname, lastname) "
                    + "VALUES (?,?,?)", "clun2@gmail.com", "Cedrick", "Lunven");
            LOGGER.info("+ Insert and externalize var with ?");
            
            // #2.b You can also externalize variables setting a label like :name
            session.execute(""
                    + "INSERT INTO users (email, firstname, lastname) "
                    + "VALUES (:e,:f,:l)", ImmutableMap.<String, Object>of(
                                                "e", "clun3@gmail.com",
                                                "f", "Cedrick", 
                                                "l", "Lunven"));
            LOGGER.info("+ Insert and externalize var with :labels");
            
            // #3. You query is marshalled as 'Statement'
            // You can create it explicitely in order to override some parameters
            // doc: https://docs.datastax.com/en/developer/java-driver/3.0/manual/statements/simple/
            SimpleStatement statement = new SimpleStatement(""
                    + "INSERT INTO users (email, firstname, lastname) "
                    + "VALUES ('clun4@sample.com', 'Cedrick', 'Lunven')");
            statement.setConsistencyLevel(ConsistencyLevel.ONE);
            session.execute(statement);
            LOGGER.info("+ Insert with explicit statements");
            
            // #4. You can use QueryBuilder to help you building your statements
            // doc: https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/querybuilder/QueryBuilder.html
            session.execute(QueryBuilder.insertInto("users")
                    .value("email", "clun5@gmail.com")
                    .value("firstname", "Cedrick")
                    .value("lastname", "Lunven"));
            LOGGER.info("+ Insert with QueryBuilder");
            
            // #5.It is recommended to prepare your statements
            // Prepare once, execute multiple times is much faster
            // Use session.prepare(<your_query>)
            // doc: https://docs.datastax.com/en/developer/java-driver/3.8/manual/statements/built/
            
            // 5.a Use '?' for parameters
            // doc: https://docs.datastax.com/en/developer/java-driver/3.8/manual/statements/prepared/
            PreparedStatement ps1 = session.prepare(""
                    + "INSERT INTO users (email, firstname, lastname) "
                    + "VALUES (?,?,?)");
            BoundStatement bs1 = ps1.bind("clun6@gmail.com", "Cedrick", "Lunven");
            session.execute(bs1);
            LOGGER.info("+ Insert with preparing the statement");
            
            // 5.b To prepare statements with QueryBuilder, use 'bindMarker'
            PreparedStatement ps2 = session.prepare(QueryBuilder.insertInto("users")
                        .value("email", QueryBuilder.bindMarker())
                        .value("firstname", QueryBuilder.bindMarker())
                        .value("lastname", QueryBuilder.bindMarker()));
            session.execute(ps2.bind("clun7@gmail.com", "Cedrick", "Lunven"));
            LOGGER.info("+ Insert with PrepareStatements + QueryBuilder");
            
            
            // In next SAMPLES you will find everything with QueryBuidler and PreparedStatement
            // Enjoy !!!
            
        } finally {
            closeSessionAndCluster(session, cluster);
        }
        System.exit(0);
    }
    
   
  
}
