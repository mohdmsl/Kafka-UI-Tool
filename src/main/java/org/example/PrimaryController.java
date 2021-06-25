package org.example;

import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import org.apache.kafka.clients.admin.AdminClient;
import org.example.models.Kafka;
import org.example.topic.TopicUtil;
import org.example.utility.TopicUtils;
import org.example.utility.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;

public class PrimaryController {

    @FXML
    TextField btServer;
    @FXML
    ListView<String> topics;
    @FXML
    ChoiceBox formatter;

    TopicUtil topicUtil;

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
            Kafka.topicName = topics.getSelectionModel().getSelectedItem();
            openMenu();
        }

    }

    private void openMenu() {

        //System.out.println("dragged");
        ContextMenu cMenu = new ContextMenu();
        MenuItem topicInfo = new MenuItem("topic Info");
        MenuItem dataMenu = new MenuItem("display data");
        MenuItem deleteMenu = new MenuItem("delete Topic");
        topicInfo.setOnAction((event) -> {
            try {
                App.setRoot("topicInfo");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        dataMenu.setOnAction(actionEvent -> {
            try {
                App.setRoot("secondary");
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        deleteMenu.setOnAction(event -> {
            topicUtil = new TopicUtil();
            String topic = topics.getSelectionModel().getSelectedItem();
            AdminClient adminClient = AdminClient.create(Utils.config);
            topicUtil.deleteTopic(topic, adminClient);
        });
        cMenu.getItems().addAll(topicInfo, dataMenu,deleteMenu);
        topics.setContextMenu(cMenu);
    }


}
