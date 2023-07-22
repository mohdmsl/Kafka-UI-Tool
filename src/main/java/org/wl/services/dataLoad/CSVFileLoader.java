package org.wl.services.dataLoad;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

//Read csv file for sending to kafka topic
public class CSVFileLoader {
    private boolean containsHeader = true;
    private char separator = ',';
    private char quoteChar = '"';
    private char escapeChar = '\\';
    private String lineSeparator = "\n";

    public CSVFileLoader() {
    }

    public CSVFileLoader(boolean containsHeader, char separator, char quoteChar,
                         char escapeChar, String lineSeparator) {
        this.containsHeader = containsHeader;
        this.separator = separator;
        this.quoteChar = quoteChar;
        this.escapeChar = escapeChar;
        this.lineSeparator = lineSeparator;
    }

    public boolean isContainsHeader() {
        return containsHeader;
    }

    public void setContainsHeader(boolean containsHeader) {
        this.containsHeader = containsHeader;
    }

    public char getSeparator() {
        return separator;
    }

    public void setSeparator(char separator) {
        this.separator = separator;
    }

    public char getQuoteChar() {
        return quoteChar;
    }

    public void setQuoteChar(char quoteChar) {
        this.quoteChar = quoteChar;
    }

    public char getEscapeChar() {
        return escapeChar;
    }

    public void setEscapeChar(char escapeChar) {
        this.escapeChar = escapeChar;
    }

    public String getLineSeparator() {
        return lineSeparator;
    }

    public void setLineSeparator(String lineSeparator) {
        this.lineSeparator = lineSeparator;
    }

    public CsvSchema createCSVSchema() throws IOException {

        return CsvSchema.builder()
                .setUseHeader(this.containsHeader)
                .setColumnSeparator(this.separator)
                .setQuoteChar(this.quoteChar)
                .setEscapeChar(this.escapeChar)
                .setLineSeparator(this.lineSeparator)
                .build();

    }

    /**
     * reads csv file content and converts to json
     *
     * @param csvFile
     * @return
     */
    public List<String> readCSVFileInJson(File csvFile) {
        CsvSchema csvSchema = null;
        List<String> records = new ArrayList<String>();
        try {
            csvSchema = createCSVSchema();
            CsvMapper csvMapper = new CsvMapper();

            MappingIterator<Object> iterator = csvMapper.readerFor(Map.class).with(csvSchema).readValues(csvFile);
            while (iterator.hasNext()) {
                Object row = iterator.next();
                ObjectMapper mapper = new ObjectMapper();
                String json = mapper.writeValueAsString(row);
                records.add(json);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return records;

    }

    /**
     * reads csv file content as list of string
     *
     * @param filePath
     * @return
     */
    public List<String> readCSVFileAsText(String filePath) {
        List<String> records = new ArrayList<String>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            if (this.containsHeader && (line = reader.readLine()) != null) {
                line = null;
            }

            while ((line = reader.readLine()) != null) {
                records.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return records;

    }

}
