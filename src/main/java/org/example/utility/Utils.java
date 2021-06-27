package org.example.utility;

import org.apache.kafka.common.config.SslConfigs;
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

    public Properties getSSLProperties() {
        Properties config = new Properties();
        InputStream reader = null;
        try {
            reader = ClassLoader.getSystemClassLoader().getResourceAsStream("kafka.properties");
            config.load(reader);
            config.put("bootstrap.servers", Kafka.bootstrapServer);
            config.put("security.protocol", Kafka.mechanism);
            config.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, Kafka.tsLocation);
            config.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, Kafka.tsPassword);
            config.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, Kafka.ksLocation);
            config.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, Kafka.ksPassword);
            config.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, Kafka.keyPassword);
            config.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS");
            config.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "JKS");
            config.put(SslConfigs.SSL_CLIENT_AUTH_CONFIG, "required");
            config.put("group.id", String.valueOf(System.currentTimeMillis()));
            config.put("security.inter.broker.protocol", "SSL");
            if (!Kafka.checkbox)
                config.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            else
                config.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");

        } catch (IOException e) {
            e.printStackTrace();
        }

        return config;
    }

    public Properties getConfig() {
        Properties config;
        switch (Kafka.mechanism) {
            case "PLAINTEXT":
                config = getKafkaProps();
                break;
            case "SSL":
                config = getSSLProperties();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + Kafka.mechanism);
        }
        return config;
    }
}
