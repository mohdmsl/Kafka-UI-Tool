package org.wl.dataload;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.wl.services.dataLoad.AvroFileLoader;
import org.wl.utility.JsonUtils;

import java.io.File;
import java.util.List;
import java.util.Optional;

public class AvroFileReaderTest {
    @Test
    @DisplayName("Read Avro file")
    public void readAvroFileTest() {
        AvroFileLoader loader = new AvroFileLoader();
        List<String> list = loader.readAvroFile(new File("src/test/resources/testFiles/sample.avro"));
        Optional<String> jsonData = list.stream().filter(data -> {
            JsonNode jsonNode = new JsonUtils().getJsOnNode(data);
            if (jsonNode != null) {
                String username = jsonNode.get("username").asText();
                return username.equals("miguno");
            }
            return false;

        }).findFirst();
        Assertions.assertEquals(true, jsonData.isPresent());
    }
}
