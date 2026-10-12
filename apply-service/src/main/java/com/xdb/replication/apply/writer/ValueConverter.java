package com.xdb.replication.apply.writer;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.Base64;

/**
 * Converts Debezium's JSON values into JDBC values for the target column type.
 *
 * <p>Connectors are configured with {@code time.precision.mode=connect} and
 * {@code decimal.handling.mode=string}, so the JSON carries:
 * <ul>
 *   <li>DATE: days since epoch (int)</li>
 *   <li>TIME: milliseconds since midnight (int)</li>
 *   <li>TIMESTAMP: milliseconds since epoch (long)</li>
 *   <li>TIMESTAMP WITH TIME ZONE: ISO-8601 string</li>
 *   <li>NUMERIC/DECIMAL: string, so no precision is lost</li>
 *   <li>BINARY: base64 string</li>
 * </ul>
 * The target's own column type (read from JDBC metadata) decides the conversion, which is
 * what makes heterogeneous replication work (e.g. Postgres {@code numeric} into Oracle {@code NUMBER}).
 */
public final class ValueConverter {

    private ValueConverter() {
    }

    public static Object toJdbc(Object value, int jdbcType) {
        if (value == null) {
            return null;
        }
        return switch (jdbcType) {
            case Types.DATE -> toDate(value);
            case Types.TIME, Types.TIME_WITH_TIMEZONE -> value instanceof Number n ? new Time(n.longValue()) : value.toString();
            case Types.TIMESTAMP -> toTimestamp(value);
            case Types.TIMESTAMP_WITH_TIMEZONE -> toOffsetDateTime(value);
            case Types.NUMERIC, Types.DECIMAL -> toBigDecimal(value);
            case Types.BIGINT -> value instanceof Number n ? n.longValue() : Long.valueOf(value.toString());
            case Types.INTEGER, Types.SMALLINT, Types.TINYINT ->
                    value instanceof Number n ? n.intValue() : Integer.valueOf(value.toString());
            case Types.DOUBLE, Types.FLOAT, Types.REAL ->
                    value instanceof Number n ? n.doubleValue() : Double.valueOf(value.toString());
            case Types.BOOLEAN, Types.BIT -> toBoolean(value);
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB ->
                    value instanceof String s ? Base64.getDecoder().decode(s) : value;
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.LONGNVARCHAR,
                 Types.CLOB, Types.NCLOB -> value instanceof String ? value : value.toString();
            default -> value;
        };
    }

    private static Object toDate(Object v) {
        if (v instanceof Number n) {
            long x = n.longValue();
            // Small values are epoch days (Date logical type); large ones are epoch millis
            // (e.g. an Oracle DATE, which carries a time part and is emitted as a timestamp).
            return Math.abs(x) < 1_000_000 ? Date.valueOf(LocalDate.ofEpochDay(x)) : new Date(x);
        }
        return Date.valueOf(LocalDate.parse(v.toString().substring(0, 10)));
    }

    private static Object toTimestamp(Object v) {
        if (v instanceof Number n) {
            return new Timestamp(n.longValue());
        }
        String s = v.toString();
        try {
            return Timestamp.from(OffsetDateTime.parse(s).toInstant());
        } catch (DateTimeParseException e) {
            try {
                return Timestamp.from(Instant.parse(s));
            } catch (DateTimeParseException e2) {
                return Timestamp.valueOf(LocalDateTime.parse(s));
            }
        }
    }

    private static Object toOffsetDateTime(Object v) {
        if (v instanceof Number n) {
            return OffsetDateTime.ofInstant(Instant.ofEpochMilli(n.longValue()), java.time.ZoneOffset.UTC);
        }
        return OffsetDateTime.parse(v.toString());
    }

    private static BigDecimal toBigDecimal(Object v) {
        if (v instanceof BigDecimal b) {
            return b;
        }
        if (v instanceof Boolean b) {
            return b ? BigDecimal.ONE : BigDecimal.ZERO;
        }
        return new BigDecimal(v.toString());
    }

    private static Boolean toBoolean(Object v) {
        if (v instanceof Boolean b) {
            return b;
        }
        if (v instanceof Number n) {
            return n.intValue() != 0;
        }
        String s = v.toString();
        return "true".equalsIgnoreCase(s) || "1".equals(s) || "t".equalsIgnoreCase(s) || "y".equalsIgnoreCase(s);
    }
}
