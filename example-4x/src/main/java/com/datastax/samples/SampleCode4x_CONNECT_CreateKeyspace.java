package com.datastax.samples;

/**
 * Sample code created a keyspace with Simple Strategy, replication factor 1
 * 
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 */
public class SampleCode4x_CONNECT_CreateKeyspace implements ExampleSchema {
    
    /** StandAlone (vs JUNIT) to help you running. */
    public static void main(String[] args) {
        ExampleUtils.createKeyspace();
        System.exit(0);
    }
     
}
