package com.xdb.replication.common.events;

import com.xdb.replication.common.model.DbType;
import com.xdb.replication.common.model.RowFilter;
import com.xdb.replication.common.model.TopicRouting;

import java.time.Instant;
import java.util.List;

/**
 * Full publication state, published to {@link Topics#CONFIG_PUBLICATIONS} (key = name).
 * A {@link ConfigEventType#DELETED} event is followed by a tombstone so compaction removes it.
 */
public record PublicationEvent(
        ConfigEventType type,
        String name,
        String sourceDatabase,
        DbType sourceDbType,
        List<String> tables,
        List<RowFilter> rowFilters,
        String topicPrefix,
        TopicRouting topicRouting,
        String status,
        Instant occurredAt) {

    public PublicationEvent {
        tables = tables == null ? List.of() : List.copyOf(tables);
        rowFilters = rowFilters == null ? List.of() : List.copyOf(rowFilters);
    }
}
