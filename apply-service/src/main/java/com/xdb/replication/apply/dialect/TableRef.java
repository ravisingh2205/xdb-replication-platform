package com.xdb.replication.apply.dialect;

/** A resolved target table, using the exact identifier case found in the target's catalog. */
public record TableRef(String schema, String table) {

    public boolean hasSchema() {
        return schema != null && !schema.isBlank();
    }

    @Override
    public String toString() {
        return hasSchema() ? schema + "." + table : table;
    }
}
