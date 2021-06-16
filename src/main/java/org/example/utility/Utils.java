package org.example.utility;

import org.example.consumer.Consumer;
import org.example.models.Kafka;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Utils {

     public Properties getKafkaProps() {
        InputStream reader = null;
        Properties config = new Properties();
        try {
            reader = ClassLoader.getSystemClassLoader().getResourceAsStream("kafka.properties");
            config.load(reader);
            config.put("bootstrap.servers", Kafka.bootstrapServer);
            // System.out.println(config);
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return config;
    }
}
