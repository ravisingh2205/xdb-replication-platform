package com.xdb.replication.common.model;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RowFilterTest {

    @Test
    void inFilterMatchesCaseInsensitiveColumn() {
        RowFilter f = new RowFilter("public.customer", "region", RowFilter.Operator.IN, List.of("APAC", "EMEA"));
        assertThat(f.matches(Map.of("REGION", "APAC"))).isTrue();
        assertThat(f.matches(Map.of("region", "AMER"))).isFalse();
    }

    @Test
    void notInExcludesListedValues() {
        RowFilter f = new RowFilter("*", "status", RowFilter.Operator.NOT_IN, List.of("CANCELLED"));
        assertThat(f.matches(Map.of("status", "PAID"))).isTrue();
        assertThat(f.matches(Map.of("status", "CANCELLED"))).isFalse();
    }

    @Test
    void missingColumnNeverMatches() {
        RowFilter f = new RowFilter("*", "region", RowFilter.Operator.NE, List.of("X"));
        assertThat(f.matches(Map.of("id", 1))).isFalse();
    }

    @Test
    void appliesToWildcardAndQualifiedName() {
        assertThat(new RowFilter("*", "c", RowFilter.Operator.EQ, List.of("1")).appliesTo("a.b")).isTrue();
        assertThat(new RowFilter("public.orders", "c", RowFilter.Operator.EQ, List.of("1")).appliesTo("PUBLIC.ORDERS")).isTrue();
        assertThat(new RowFilter("public.orders", "c", RowFilter.Operator.EQ, List.of("1")).appliesTo("public.payment")).isFalse();
    }

    @Test
    void debeziumOpMapping() {
        assertThat(Operation.fromDebezium("r")).isEqualTo(Operation.SNAPSHOT);
        assertThat(Operation.fromDebezium("d").isUpsert()).isFalse();
        assertThat(Operation.fromDebezium("u").isUpsert()).isTrue();
    }
}
