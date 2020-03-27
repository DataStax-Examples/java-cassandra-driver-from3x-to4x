package com.datastax.samples;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

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
public class SampleCode3x_CONNECT_Policies implements ExampleSchema {
    
    /** Logger for the class. */
    private static Logger LOGGER = LoggerFactory.getLogger(SampleCode3x_CONNECT_Policies.class);
    
    /** 
     * StandAlone program relying on main method to easy copy/paste.
     */
    public static void main(String[] args) {
        LOGGER.info("Starting 'Policies' sample...");
        
        /**
         * Load-Balancing Policies
         * https://docs.datastax.com/en/developer/java-driver/3.8/manual/load_balancing/
         */
        // Default
        // https://docs.datastax.com/en/developer/java-driver/3.8/manual/load_balancing/#token-aware-policy
        LoadBalancingPolicy lbDefault    = new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build());
        LOGGER.info("[LoadBalancing] - {}", lbDefault.toString());
        // RoundRobin
        // https://docs.datastax.com/en/developer/java-driver/3.8/manual/load_balancing/#round-robin-policy
        LoadBalancingPolicy lbRoundRobin = new RoundRobinPolicy();
        LOGGER.info("[LoadBalancing] - {}", lbRoundRobin.toString());
        // DataCenter aware
        // https://docs.datastax.com/en/developer/java-driver/3.8/manual/load_balancing/#dc-aware-round-robin-policy
        LoadBalancingPolicy lbLocalDc    = DCAwareRoundRobinPolicy.builder().withLocalDc("datacenter1").build();
        LOGGER.info("[LoadBalancing] - {}", lbLocalDc.toString());
        // Latency aware
        // https://docs.datastax.com/en/developer/java-driver/3.8/manual/load_balancing/#latency-aware-policy
        LoadBalancingPolicy lbLatency = LatencyAwarePolicy.builder(lbLocalDc)
                                                .withExclusionThreshold(2.0)
                                                .withScale(100, TimeUnit.MILLISECONDS)
                                                .withRetryPeriod(10, TimeUnit.SECONDS)
                                                .withUpdateRate(100, TimeUnit.MILLISECONDS)
                                                .withMininumMeasurements(50)
                                                .build();
        LOGGER.info("[LoadBalancing] - {}", lbLatency.toString());
        
        /**
         * Retry Policies
         * https://docs.datastax.com/en/developer/java-driver/3.8/manual/retries/#retry-policy
         * https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/policies/RetryPolicy.html
         */
        RetryPolicy retryDefault = DefaultRetryPolicy.INSTANCE;
        LOGGER.info("[Retry] - {}", retryDefault.toString());
        // https://docs.datastax.com/en/drivers/java/3.8/com/datastax/driver/core/policies/FallthroughRetryPolicy.html
        RetryPolicy retryFall = FallthroughRetryPolicy.INSTANCE;
        LOGGER.info("[Retry] - {}", retryFall.toString());
        RetryPolicy retryLogged = new LoggingRetryPolicy(retryFall);
        LOGGER.info("[Retry] - {}", retryLogged.toString());
        
        /**
         * Reconnection Policies
         * https://docs.datastax.com/en/developer/java-driver/3.8/manual/reconnection/
         */
        ReconnectionPolicy reconnExpo = new ExponentialReconnectionPolicy(1000, 10 * 60 * 1000);
        LOGGER.info("[Reconnection] - {}", reconnExpo.toString());
        ReconnectionPolicy reconnConst = new ConstantReconnectionPolicy(20);
        LOGGER.info("[Reconnection] - {}", reconnConst.toString());
        
        
        
        try(Cluster cluster = Cluster.builder()
                .addContactPoint("127.0.0.1")
                .withLoadBalancingPolicy(lbDefault)
                .withRetryPolicy(retryDefault)
                .withReconnectionPolicy(reconnExpo)
                .build()) {
            LOGGER.info("Connected to Cluster");
        }
        LOGGER.info("[OK] Success");
        System.exit(0);
    }
     
}
