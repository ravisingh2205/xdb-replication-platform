package com.xdb.replication.common.model;

/**
 * How a publication's tables map to Kafka topics. Chosen per publication to keep the
 * partition count sane for publications with hundreds of tables.
 *
 * <p>Ordering per row holds in every mode: the record key is the primary key (plus a
 * table identifier when tables share a topic), so a row's changes always land on one partition.
 */
public enum TopicRouting {

    /** {@code cdc.<pub>.<schema>.<table>}: one topic per table. Best isolation; default. */
    PER_TABLE,

    /** {@code cdc.<pub>.<schema>}: one topic per source schema (ByLogicalTableRouter SMT). */
    PER_SCHEMA,

    /** {@code cdc.<pub>.all}: one topic for the whole publication, sized by partition count. */
    SINGLE
}
