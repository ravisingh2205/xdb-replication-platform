package com.xdb.replication.common.events;

public enum ConfigEventType {
    /** Created or changed; the event carries the complete current state. */
    UPSERT,
    DELETED
}
