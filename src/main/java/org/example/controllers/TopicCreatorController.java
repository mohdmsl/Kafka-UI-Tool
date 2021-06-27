package org.example.controllers;

import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.TextField;
import org.apache.kafka.clients.admin.AdminClient;
import org.example.topic.Topic;
import org.example.utility.Utils;

import java.util.Properties;

public class TopicCreatorController {
    @FXML
    private TextField name;
    @FXML
    private TextField partitions;
    @FXML
    private TextField replicas;
    @FXML
    private Button create;

    @FXML
    private void createTopic(){
        Properties config = new Utils().getConfig();
        AdminClient adminClient = AdminClient.create(config);
        Topic topic = new Topic(adminClient);
        int partition = Integer.parseInt( partitions.getText());
        short replica = Short.parseShort(replicas.getText());
        topic.createTopic(name.getText(),partition,replica);
    }

}
