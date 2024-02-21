package com.datastax.samples;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.datastax.samples.ExampleUtils.closeSession;
import static com.datastax.samples.ExampleUtils.connect;
import static com.datastax.samples.ExampleUtils.createKeyspace;

public class SampleCode4x_Vector {

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleCode4x_CRUD_11_Reactive.class);

    private static CqlSession session;

    public static void main(String[] args)
    throws InterruptedException, ExecutionException {

        try {

            // === INITIALIZING ===

            // Create keyspace (if needed)
            createKeyspace();

            // Initialize Cluster and Session Objects (connected to keyspace)
            session = connect();

            // Create table and index.
            createSchema(session);

            // Populate Table
            populateTable(session);

            // Semantic Search
            findProductById(session, "pf1843").ifPresent(product -> {
                LOGGER.info("Product Found ! looking for similar products");
                findAllSimilarProducts(session, product).forEach(p -> LOGGER.info("Product {}", p));
            });

        } finally {
            closeSession(session);

        }
    }

    private static record Product(String productId, String productName, Object vector) {}

    private static void createSchema(CqlSession cqlSession) {
        // Create a Table with Embeddings
        cqlSession.execute(
                "CREATE TABLE IF NOT EXISTS pet_supply_vectors (" +
                "    product_id     TEXT PRIMARY KEY," +
                "    product_name   TEXT," +
                "    product_vector vector<float, 14>)");
        LOGGER.info("Table created.");

        // Create a Search Index
        cqlSession.execute(
                "CREATE CUSTOM INDEX IF NOT EXISTS idx_vector " +
                "ON pet_supply_vectors(product_vector) " +
                "USING 'StorageAttachedIndex'");
        LOGGER.info("Index Created.");
    }

    private static void populateTable(CqlSession cqlSession) {
        // Insert rows
        cqlSession.execute(
                "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                "VALUES ('pf1843','HealthyFresh - Chicken raw dog food',[1, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0])");
        cqlSession.execute(
                "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                "VALUES ('pf1844','HealthyFresh - Beef raw dog food',[1, 0, 1, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 0])");
        cqlSession.execute(
                "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                "VALUES ('pt0021','Dog Tennis Ball Toy',[0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0])");
        cqlSession.execute(
                "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                "VALUES ('pt0041','Dog Ring Chew Toy',[0, 0, 0, 1, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0])");
        cqlSession.execute(
                "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                "VALUES ('pf7043','PupperSausage Bacon dog Treats',[0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 1, 1])");
        cqlSession.execute(
                "INSERT INTO pet_supply_vectors (product_id, product_name, product_vector) " +
                "VALUES ('pf7044','PupperSausage Beef dog Treats',[0, 0, 0, 1, 0, 1, 1, 0, 0, 0, 0, 0, 1, 0])");
        LOGGER.info("Rows inserted.");
    }

    private static Optional<Product> findProductById(CqlSession cqlsession, String productId) {
        Row row = cqlsession.execute(SimpleStatement
                .builder("SELECT * FROM pet_supply_vectors WHERE product_id = ?")
                .addPositionalValue(productId).build()).one();
        return (row != null) ? Optional.of(row).map(SampleCode4x_Vector::mapRowAsProduct) : Optional.empty();
    }

    private static List<Product> findAllSimilarProducts(CqlSession cqlsession, Product orginal) {
        return cqlsession.execute(SimpleStatement
                        .builder("SELECT * FROM pet_supply_vectors " +
                                "ORDER BY product_vector ANN OF ? LIMIT 2;")
                        .addPositionalValue(orginal.vector)
                        .build())
                .all()
                .stream()
                .filter(row -> !row.getString("product_id").equals(orginal.productId))
                .map(SampleCode4x_Vector::mapRowAsProduct)
                .toList();
    }

    private static Product mapRowAsProduct(Row row) {
        return new Product(
                row.getString("product_id"),
                row.getString("product_name"),
                row.getObject("product_vector"));
    }


}
