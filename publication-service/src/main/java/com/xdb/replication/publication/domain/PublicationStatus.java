package com.xdb.replication.publication.domain;

/** Lifecycle of a publication, mirrored from its Debezium connector's state. */
public enum PublicationStatus {
    /** Saved; connector registration not confirmed yet. */
    PROVISIONING,
    /** Connector and its tasks are RUNNING (snapshotting or streaming). */
    RUNNING,
    PAUSED,
    /** Connector or a task FAILED; the reconciler will try to restart it. */
    FAILED
}
