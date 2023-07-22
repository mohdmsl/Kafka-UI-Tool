package org.wl.dataload;

import org.wl.services.dataLoad.JsonFileLoader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

public class JsonFileLoaderTest {

    @Test
    @DisplayName("Read Json file")
    public void readJsonFileTest() {
        JsonFileLoader loader = new JsonFileLoader();
        List<String> list = loader.readJsonFile(new File("src/test/resources/testFiles/dm.json"));
        Assertions.assertEquals(5, list.size());
    }
}
