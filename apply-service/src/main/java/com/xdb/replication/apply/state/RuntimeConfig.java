package com.xdb.replication.apply.state;

import com.xdb.replication.common.events.PublicationEvent;
import com.xdb.replication.common.events.SubscriptionEvent;
import com.xdb.replication.common.model.TableMapping;

import java.util.Optional;

/**
 * Immutable snapshot of everything one subscription's apply loop needs: which publication
 * (topics, row filters), which target DB, and which table mappings. Swapped atomically when
 * a config event arrives, so mapping or filter changes take effect on the next batch
 * without restarting the consumer.
 */
public record RuntimeConfig(SubscriptionEvent subscription, PublicationEvent publication) {

    public String name() {
        return subscription.name();
    }

    public Optional<TableMapping> mappingFor(String schema, String table) {
        return subscription.tableMappings().stream().filter(m -> m.matchesSource(schema, table)).findFirst();
    }

    /** True when the change needs a new consumer and connection pool rather than a config swap. */
    public boolean requiresRestart(RuntimeConfig next) {
        return !subscription.publication().equals(next.subscription.publication())
                || !subscription.target().equals(next.subscription.target());
    }
}
