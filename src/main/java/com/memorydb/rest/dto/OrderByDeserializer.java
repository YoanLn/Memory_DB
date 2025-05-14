package com.memorydb.rest.dto;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Désérialiseur personnalisé pour le champ orderBy
 * Supporte à la fois une chaîne simple (compatibilité) et un tableau d'objets OrderByDto
 */
public class OrderByDeserializer extends JsonDeserializer<List<OrderByDto>> {
    @Override
    public List<OrderByDto> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        JsonToken token = p.getCurrentToken();
        
        // Si c'est une chaîne simple, on la convertit en un OrderByDto
        if (token == JsonToken.VALUE_STRING) {
            String columnName = p.getText();
            List<OrderByDto> result = new ArrayList<>();
            result.add(new OrderByDto(columnName, true)); // Tri ascendant par défaut
            return result;
        }
        
        // Sinon, on laisse Jackson faire la désérialisation normale
        return ctxt.readValue(p, ctxt.getTypeFactory().constructCollectionType(List.class, OrderByDto.class));
    }
}
