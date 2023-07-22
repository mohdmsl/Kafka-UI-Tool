package org.wl.dataload;


import org.wl.services.dataLoad.CSVFileLoader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;

public class CSVFileLoaderTester {
    @Test
    @DisplayName("Read csv file")
    public void readCsvFileTest() {
        CSVFileLoader loader = new CSVFileLoader();
        List<String> list = loader.readCSVFileInJson(new File("src/test/resources/testFiles/dm.csv"));
        Assertions.assertEquals(10, list.size());
    }

    @Test
    @DisplayName("pipe delimited")
    public void testPipeDelimited() {
        CSVFileLoader loader = new CSVFileLoader();
        loader.setSeparator('|');
        List<String> list = loader.readCSVFileInJson(new File("src/test/resources/testFiles/dmPipe.csv"));
        Assertions.assertEquals(10, list.size());

    }

    @Test
    @DisplayName("Read csv file as text")
    public void readAsText() {
        CSVFileLoader loader = new CSVFileLoader();
        List<String> list = loader.readCSVFileAsText("src/test/resources/testFiles/dm.csv");
        Assertions.assertEquals(10, list.size());
    }

}
