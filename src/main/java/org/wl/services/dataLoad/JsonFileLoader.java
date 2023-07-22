package org.wl.services.dataLoad;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class JsonFileLoader {
    private static final Logger logger = LogManager.getLogger(JsonFileLoader.class);

    public List<String> readJsonFile(File file){
        List<String> records = new ArrayList<String>();
        try {
            logger.info("reading file json file from " + file.getAbsolutePath() );
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(file);

            if (rootNode.isArray()) {
                for (JsonNode recordNode : rootNode) {
                    String json = recordNode.toString();
                    records.add(json);
                }
            }
            else{
                String json = rootNode.toString();
                records.add(json);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
        return records;
    }
}
