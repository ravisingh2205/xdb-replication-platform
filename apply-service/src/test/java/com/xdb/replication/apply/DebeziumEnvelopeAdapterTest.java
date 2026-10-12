package com.xdb.replication.apply;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xdb.replication.apply.adapter.DebeziumEnvelopeAdapter;
import com.xdb.replication.apply.consumer.RowFilterEvaluator;
import com.xdb.replication.apply.consumer.RowFilterEvaluator.Decision;
import com.xdb.replication.common.model.ChangeEvent;
import com.xdb.replication.common.model.Operation;
import com.xdb.replication.common.model.RowFilter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DebeziumEnvelopeAdapterTest {

    private final DebeziumEnvelopeAdapter adapter = new DebeziumEnvelopeAdapter(new ObjectMapper());

    private static ConsumerRecord<String, String> record(String key, String value) {
        return new ConsumerRecord<>("cdc.orders_pub.public.customer", 2, 41L, key, value);
    }

    private static final String UPDATE = """
            {"before":{"id":7,"name":"Asha","region":"APAC"},
             "after":{"id":7,"name":"Asha","region":"EMEA"},
             "source":{"connector":"postgresql","schema":"public","table":"customer","txId":991,"ts_ms":1700000000000},
             "op":"u","ts_ms":1700000000100}""";

    @Test
    void adaptsPostgresUpdate() {
        ChangeEvent e = adapter.adapt(record("{\"id\":7}", UPDATE)).orElseThrow();
        assertThat(e.operation()).isEqualTo(Operation.UPDATE);
        assertThat(e.qualifiedSourceTable()).isEqualTo("public.customer");
        assertThat(e.key()).containsEntry("id", 7);
        assertThat(e.after()).containsEntry("region", "EMEA");
        assertThat(e.transactionId()).isEqualTo("991");
        assertThat(e.partition()).isEqualTo(2);
        assertThat(e.offset()).isEqualTo(41L);
    }

    @Test
    void acceptsEnvelopesWithEmbeddedSchema() {
        String withSchema = "{\"schema\":{},\"payload\":" + UPDATE + "}";
        assertThat(adapter.adapt(record("{\"schema\":{},\"payload\":{\"id\":7}}", withSchema)).orElseThrow().key())
                .containsEntry("id", 7);
    }

    @Test
    void tombstonesAndNonChangeMessagesAreIgnored() {
        assertThat(adapter.adapt(record("{\"id\":7}", null))).isEmpty();
        assertThat(adapter.adapt(record(null, "{\"ts_ms\":1}"))).isEmpty();
    }

    @Test
    void garbageIsMalformed() {
        assertThatThrownBy(() -> adapter.adapt(record(null, "not json")))
                .isInstanceOf(DebeziumEnvelopeAdapter.MalformedEventException.class);
    }

    @Test
    void rowLeavingTheFilterBecomesADelete() {
        ChangeEvent e = adapter.adapt(record("{\"id\":7}", UPDATE)).orElseThrow();
        var apac = List.of(new RowFilter("public.customer", "region", RowFilter.Operator.IN, List.of("APAC")));
        assertThat(RowFilterEvaluator.evaluate(e, apac)).isEqualTo(Decision.DELETE_INSTEAD);

        var emea = List.of(new RowFilter("public.customer", "region", RowFilter.Operator.EQ, List.of("EMEA")));
        assertThat(RowFilterEvaluator.evaluate(e, emea)).isEqualTo(Decision.APPLY);

        var amer = List.of(new RowFilter("public.customer", "region", RowFilter.Operator.EQ, List.of("AMER")));
        assertThat(RowFilterEvaluator.evaluate(e, amer)).isEqualTo(Decision.SKIP);

        var otherTable = List.of(new RowFilter("public.orders", "region", RowFilter.Operator.EQ, List.of("X")));
        assertThat(RowFilterEvaluator.evaluate(e, otherTable)).isEqualTo(Decision.APPLY);
    }
}
