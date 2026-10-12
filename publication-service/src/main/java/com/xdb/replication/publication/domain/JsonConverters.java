package com.xdb.replication.publication.domain;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xdb.replication.common.model.RowFilter;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.util.ArrayList;
import java.util.List;

/** Stores small value collections as JSON text columns (portable across RDS Postgres versions). */
public final class JsonConverters {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonConverters() {
    }

    abstract static class JsonListConverter<T> implements AttributeConverter<List<T>, String> {
        private final TypeReference<List<T>> type;

        JsonListConverter(TypeReference<List<T>> type) {
            this.type = type;
        }

        @Override
        public String convertToDatabaseColumn(List<T> value) {
            try {
                return MAPPER.writeValueAsString(value == null ? List.of() : value);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public List<T> convertToEntityAttribute(String json) {
            try {
                return json == null || json.isBlank() ? new ArrayList<>() : MAPPER.readValue(json, type);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Converter
    public static class StringListConverter extends JsonListConverter<String> {
        public StringListConverter() {
            super(new TypeReference<>() { });
        }
    }

    @Converter
    public static class RowFilterListConverter extends JsonListConverter<RowFilter> {
        public RowFilterListConverter() {
            super(new TypeReference<>() { });
        }
    }
}
