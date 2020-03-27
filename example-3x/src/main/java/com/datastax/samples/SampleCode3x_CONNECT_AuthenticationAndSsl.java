package com.datastax.samples;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.SSLOptions;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

/**
 * Create a keyspace with Simple Strategy and replication factor 1 (for local environment)
 * 
 * Pre-requisites:
 * - Cassandra running locally (127.0.0.1, port 9042)
 * 
 * @author DataStax Developer Advocate Team
 * 
 * Need Help ? Join us on community.datastax.com to ask your questions for free.
 * 
 * https://docs.datastax.com/en/developer/java-driver/4.5/manual/core/load_balancing/
 */
public class SampleCode3x_CONNECT_AuthenticationAndSsl implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CONNECT_AuthenticationAndSsl.class);
    
    private static final boolean enableSSL = false;
    
    /** 
     * StandAlone program relying on main method to easy copy/paste.
     */
    public static void main(String[] args) {
        LOGGER.info("Starting 'AuthenticationAndSsl' sample...");
        
        // --- Authentication ---
        
        // Shortcut for PlainTextAuthProvider
        try(Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withCredentials("cassandra", "cassandra")
                .build()) {
            LOGGER.info("Connected to Cluster with Default AuthProvider");
        }
        
        try(Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withAuthProvider(new PlainTextAuthProvider("cassandra", "cassandra"))
                .build()) {
            LOGGER.info("Connected to Cluster with Explicit AuthProvider");
        }
        
        /* 
         * --- SSL ---
         * 
         * - client-to-node encryption, where the traffic is encrypted, and the client verifies the 
         *   identity of the Cassandra nodes it connects to
         * - optionally, client certificate authentication, where Cassandra nodes also verify the 
         *   identity of the client.
         */
        
        if (enableSSL) {
            
            try(Cluster cluster = Cluster.builder()
                    .addContactPoint("127.0.0.1")
                    .withSSL()
                    .build()) {
                LOGGER.info("Connected to Cluster with Default SSL");
            }
            
            // You can create SSL Options, SSL Context
            
            // Init a TrustManager from a Keystore
            try {
                TrustManagerFactory trustManager = 
                        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            
                KeyStore ks = KeyStore.getInstance("JKS");
                InputStream trustStore = new FileInputStream("client.truststore");
                ks.load(trustStore, "password123".toCharArray());
                trustManager.init(ks);
        
                // Netty SslContext
                SslContext sslContextNetty = SslContextBuilder.forClient()
                   // Pick a SSL Provider
                   .sslProvider(SslProvider.OPENSSL)
                   // Client authentication
                   .keyManager(new File("client.crt"), new File("client.key"))
                   // And the Trust Manager
                   .trustManager(trustManager)
                   .build();
                    
                SSLOptions sslOptionsNetty = 
                    new RemoteEndpointAwareNettySSLOptions(sslContextNetty);
                
                try(Cluster cluster = Cluster.builder()
                        .addContactPoint("127.0.0.1")
                        .withSSL(sslOptionsNetty)
                        .build()) {
                    LOGGER.info("Connected to Cluster with explicit SSL");
                }
                
            } catch (NoSuchAlgorithmException e) {
                LOGGER.error("Error in SSL", e);
            } catch (KeyStoreException e) {
                LOGGER.error("Error in SSL", e);
            } catch (SSLException e) {
                LOGGER.error("Error in SSL", e);
            } catch (CertificateException e) {
                LOGGER.error("Error in SSL", e);
            } catch (IOException e) {
                LOGGER.error("Error in SSL", e);
            }
        }
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
     
}
