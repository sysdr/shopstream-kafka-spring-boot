package com.shopstream.events;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.support.TransactionTemplate;

/** The point of the outbox: the order row and the event row commit or roll back together. */
class OutboxSchemaTest {
    @Test
    void orderAndOutboxRowRollBackTogether() {
        EmbeddedDatabase ds = new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2)
                .addScript("schema.sql").build();
        try {
            JdbcTemplate db = new JdbcTemplate(ds);
            TransactionTemplate tx = new TransactionTemplate(new DataSourceTransactionManager(ds));
            try {
                tx.executeWithoutResult(s -> {
                    db.update("INSERT INTO orders (order_id, customer_id, sku, quantity, total) VALUES ('OBX-1','C','S',1,1)");
                    db.update("INSERT INTO outbox (aggregate_id, event_type, payload) VALUES ('OBX-1','OrderPlaced','{}')");
                    throw new IllegalStateException("crash inside the transaction");
                });
            } catch (IllegalStateException expected) {
                // rolled back
            }
            assertEquals(0, db.queryForObject("SELECT COUNT(*) FROM orders", Integer.class));
            assertEquals(0, db.queryForObject("SELECT COUNT(*) FROM outbox", Integer.class));
        } finally {
            ds.shutdown();
        }
    }
}
