package org.wl.utility;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtils {

    public JsonNode  getJsOnNode(String data){
        ObjectMapper mapper = new ObjectMapper();
        // Parse JSON string to JsonNode
        JsonNode jsonNode = null;
        try {
            jsonNode = mapper.readTree(data);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return jsonNode;
    }
}
