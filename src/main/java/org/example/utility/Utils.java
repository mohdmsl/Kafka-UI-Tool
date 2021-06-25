package org.example.utility;

import org.example.models.Kafka;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Utils {
    public static Properties config;

    public Properties getKafkaProps() {
        InputStream reader = null;
        config = new Properties();
        try {
            reader = ClassLoader.getSystemClassLoader().getResourceAsStream("kafka.properties");
            config.load(reader);
            config.put("bootstrap.servers", Kafka.bootstrapServer);
            config.put("group.id", String.valueOf(System.currentTimeMillis()));
            // System.out.println(config);
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return config;
    }
}
