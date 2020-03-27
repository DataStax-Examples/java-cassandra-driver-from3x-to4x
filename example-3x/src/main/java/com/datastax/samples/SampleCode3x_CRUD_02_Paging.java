package com.datastax.samples;

import static com.datastax.samples.ExampleUtils.closeSessionAndCluster;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;
import static com.datastax.samples.ExampleUtils.createTableUser;
import static com.datastax.samples.ExampleUtils.truncateTable;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

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
public class SampleCode3x_CRUD_02_Paging implements ExampleSchema {

    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CRUD_02_Paging.class);

    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        
        Cluster cluster = null;
        Session session = null;
        try {

            // === INITIALIZING ===
            
            // Create killrvideo keyspace (if needed)
            createKeyspace();
            
            // Initialize Cluster and Session Objects 
            session = connect(cluster);
            
            // Create working table User (if needed)
            createTableUser(session);
            
            // Empty tables for tests
            truncateTable(session, USER_TABLENAME);
            
            PreparedStatement stmtCreateUser = 
                    session.prepare(QueryBuilder.insertInto(USER_TABLENAME)
                    .value(USER_EMAIL, QueryBuilder.bindMarker())
                    .value(USER_FIRSTNAME, QueryBuilder.bindMarker())
                    .value(USER_LASTNAME, QueryBuilder.bindMarker()));

            // Adding 50 records in the table
            BatchStatement batch = new BatchStatement();
            for (int i = 0; i < 50; i++) {
            	batch.add(stmtCreateUser.bind("user_" + i + "@sample.com", "user_" + i, "lastname"));
			}
            session.execute(batch);
            LOGGER.info("+ {} users have been created", 50);
            
            // Paged query
            Statement statement = QueryBuilder.select()
                .from(USER_TABLENAME)
                .setFetchSize(10)              // 10 per pages
                .setReadTimeoutMillis(1000)    // 1s timeout
                .setConsistencyLevel(ConsistencyLevel.ONE);
            ResultSet page1 = session.execute(statement);
            
            // Checking
            LOGGER.info("+ Page 1 has {} items", page1.getAvailableWithoutFetching());
            Iterator<Row> page1Iter = page1.iterator();
            while (0 <  page1.getAvailableWithoutFetching()) {
            	LOGGER.info("Page1: " + page1Iter.next().getString(USER_EMAIL));
            }
            
            // Notice that items are NOT ordered (it uses the hashed token)
            // From this point if you invoke .next() driver will look for page2.
            // But we can invoke page2 directly: (useful for delay between calls)
            String page1PagingState = page1.getExecutionInfo().getPagingState().toString();
            statement.setPagingState(PagingState.fromString(page1PagingState));
            ResultSet page2 = session.execute(statement);
            LOGGER.info("+ Page 2 has {} items", page2.getAvailableWithoutFetching());
            
        } finally {
            closeSessionAndCluster(session, cluster);
        }
        System.exit(0);
    }
   
}
