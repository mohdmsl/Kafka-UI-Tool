package org.example.topic;

import org.apache.kafka.clients.admin.AdminClient;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class TopicUtil {
    public TopicUtil() {
    }

    public Set<String> getAllTopics(Properties config) throws ExecutionException, InterruptedException {
        AdminClient adminClient = AdminClient.create(config);
        return adminClient.listTopics().names().get();
    }
}
