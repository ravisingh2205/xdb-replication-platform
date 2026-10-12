package com.xdb.replication.apply;

import com.xdb.replication.apply.writer.ValueConverter;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class ValueConverterTest {

    @Test
    void epochDaysBecomeDate() {
        assertThat(ValueConverter.toJdbc(19723, Types.DATE)).isEqualTo(Date.valueOf("2024-01-01"));
    }

    @Test
    void epochMillisBecomeTimestamp() {
        assertThat(ValueConverter.toJdbc(1704067200000L, Types.TIMESTAMP)).isEqualTo(new Timestamp(1704067200000L));
    }

    @Test
    void isoStringBecomesOffsetDateTime() {
        assertThat(ValueConverter.toJdbc("2024-01-01T10:15:30Z", Types.TIMESTAMP_WITH_TIMEZONE))
                .isEqualTo(OffsetDateTime.parse("2024-01-01T10:15:30Z"));
    }

    @Test
    void decimalStringKeepsPrecision() {
        assertThat(ValueConverter.toJdbc("12345678901234567890.123456", Types.NUMERIC))
                .isEqualTo(new BigDecimal("12345678901234567890.123456"));
    }

    @Test
    void booleanFromNumberForOracleStyleFlags() {
        assertThat(ValueConverter.toJdbc(1, Types.BOOLEAN)).isEqualTo(true);
        assertThat(ValueConverter.toJdbc(true, Types.NUMERIC)).isEqualTo(BigDecimal.ONE);
    }

    @Test
    void nullStaysNull() {
        assertThat(ValueConverter.toJdbc(null, Types.VARCHAR)).isNull();
    }
}
