package org.wl.services.dataLoad;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetFileLoader {

    private static final Logger logger = LogManager.getLogger(ParquetFileLoader.class);


    /**
     * Reads parquet File
     * @param filePath
     * @return
     */
    public List<String> readParquetFile(String filePath) {
        List<String> records = new ArrayList<String>();
        logger.info("reading json file from " + filePath );
        try {
            Configuration config = new Configuration();
            config.set("parquet.avro.readInt96AsFixed", "true");
            HadoopInputFile file = HadoopInputFile.fromPath(new Path(filePath), config);
            ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file)
                    .withConf(config)
                    .build();
            GenericRecord record;
            while ((record = reader.read()) != null) {
                String message = record.toString();
                records.add(message);
            }
            logger.info("completed reading parquet file from " + filePath );
        } catch (IOException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
        return records;
    }
}
