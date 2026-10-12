package com.xdb.replication.subscription.domain;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xdb.replication.common.model.TableMapping;
import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.util.ArrayList;
import java.util.List;

@Converter
public class TableMappingListConverter implements AttributeConverter<List<TableMapping>, String> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final TypeReference<List<TableMapping>> TYPE = new TypeReference<>() { };

    @Override
    public String convertToDatabaseColumn(List<TableMapping> value) {
        try {
            return MAPPER.writeValueAsString(value == null ? List.of() : value);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public List<TableMapping> convertToEntityAttribute(String json) {
        try {
            return json == null || json.isBlank() ? new ArrayList<>() : MAPPER.readValue(json, TYPE);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
