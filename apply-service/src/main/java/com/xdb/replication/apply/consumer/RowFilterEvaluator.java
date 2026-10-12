package com.xdb.replication.apply.consumer;

import com.xdb.replication.common.model.ChangeEvent;
import com.xdb.replication.common.model.Operation;
import com.xdb.replication.common.model.RowFilter;

import java.util.List;
import java.util.Map;

/**
 * Applies a publication's row filters to a change, including rows that move in or out
 * of the filter on UPDATE. For {@code region IN ('APAC')}:
 * <ul>
 *   <li>update EMEA to APAC: the row enters the filter, so it is upserted</li>
 *   <li>update APAC to EMEA: the row leaves the filter, so it is deleted from the target</li>
 * </ul>
 * Detecting the second case needs the before image (REPLICA IDENTITY FULL on Postgres).
 */
public final class RowFilterEvaluator {

    public enum Decision { APPLY, DELETE_INSTEAD, SKIP }

    private RowFilterEvaluator() {
    }

    public static Decision evaluate(ChangeEvent e, List<RowFilter> filters) {
        List<RowFilter> applicable = filters.stream().filter(f -> f.appliesTo(e.qualifiedSourceTable())).toList();
        if (applicable.isEmpty() || e.operation() == Operation.TRUNCATE) {
            return Decision.APPLY;
        }
        if (e.operation() == Operation.DELETE) {
            return matches(e.before(), applicable) ? Decision.APPLY : Decision.SKIP;
        }
        if (matches(e.after(), applicable)) {
            return Decision.APPLY;
        }
        if (e.operation() == Operation.UPDATE && !e.before().isEmpty() && matches(e.before(), applicable)) {
            return Decision.DELETE_INSTEAD;
        }
        return Decision.SKIP;
    }

    private static boolean matches(Map<String, Object> row, List<RowFilter> filters) {
        return row != null && !row.isEmpty() && filters.stream().allMatch(f -> f.matches(row));
    }
}
