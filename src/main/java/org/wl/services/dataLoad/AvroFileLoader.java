package org.wl.services.dataLoad;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroFileLoader {

    public List<String> readAvroFile(File file) {
        List<String> records = new ArrayList<String>();

        try {
            // Create a DatumReader for reading GenericRecords
            DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

            // Open the Avro file for reading
            FileReader<GenericRecord> fileReader = DataFileReader.openReader(file, datumReader);

            // Read records from the Avro file
            for (GenericRecord record : fileReader) {
                records.add(record.toString());
            }

            // Close the file reader
            fileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }
}
