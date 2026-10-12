package com.xdb.replication.common.events;

import com.xdb.replication.common.model.DatabaseEndpoint;
import com.xdb.replication.common.model.TableMapping;

import java.time.Instant;
import java.util.List;

/**
 * Full subscription state, published to {@link Topics#CONFIG_SUBSCRIPTIONS} (key = name).
 * The apply-service answers its three questions from this event alone:
 * which publication, which target DB, which tables/mappings.
 */
public record SubscriptionEvent(
        ConfigEventType type,
        String name,
        String publication,
        DatabaseEndpoint target,
        List<TableMapping> tableMappings,
        boolean active,
        Instant occurredAt) {

    public SubscriptionEvent {
        tableMappings = tableMappings == null ? List.of() : List.copyOf(tableMappings);
    }
}
