package org.example;

import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import org.example.models.Kafka;
import org.example.utility.TopicUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class PrimaryController {

    @FXML
    TextField btServer;
    @FXML
    ListView topics;
    @FXML
    ChoiceBox formatter;


    @FXML
    private void connect() {
        Kafka.bootstrapServer = btServer.getText();
        Properties config = new Properties();
        config.put("bootstrap.servers", Kafka.bootstrapServer);
        Set<String> topics = null;
        topics = TopicUtils.getAllTopics(config);
        showTopic(topics);


    }

    private void showTopic(Set<String> topicList) {
        // System.out.println(topicList);
        ArrayList<String> list = new ArrayList<>(topicList);
        Collections.sort(list);
        topics.getItems().addAll(list);
    }

    @FXML
    private void showData(MouseEvent event) {
        if (event.getButton() == MouseButton.SECONDARY) {
            Kafka.topicName = topics.getSelectionModel().getSelectedItem().toString();
            openMenu();
        }

    }

    private void openMenu() {

        //System.out.println("dragged");
        ContextMenu cMenu = new ContextMenu();
        MenuItem topicMenu = new MenuItem("topic Info");
        MenuItem dataMenu = new MenuItem("display data");
        topicMenu.setOnAction((event) -> {

        });
        dataMenu.setOnAction(actionEvent -> {
            try {
                App.setRoot("secondary");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        cMenu.getItems().addAll(topicMenu, dataMenu);
        topics.setContextMenu(cMenu);
    }




}
