package org.wl.dataload;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.wl.services.dataLoad.ParquetFileLoader;
import org.wl.utility.JsonUtils;

import java.util.List;
import java.util.Optional;

public class ParquetFileLoaderTest {

    @Test
    @DisplayName("Read Parquet file")
    public void readParquetTest() {
        ParquetFileLoader loader = new ParquetFileLoader();
        List<String> list = loader.readParquetFile("src/test/resources/testFiles/sample.parquet");
        Optional<String> jsonData = list.stream().filter(data -> {
            JsonNode jsonNode = new JsonUtils().getJsOnNode(data);
            if (jsonNode != null) {
                int id = jsonNode.get("id").asInt();
                return id == 75;
            }
            return false;

        }).findFirst();
        String email = "";
        if (jsonData.isPresent()) {
            JsonNode jsonNode = new JsonUtils().getJsOnNode(jsonData.get());
            if (jsonNode != null)
                email = jsonNode.get("email").asText();
        }
        Assertions.assertEquals("cgibson22@ebay.com", email);
    }
}
