package org.example.controllers;

import javafx.collections.FXCollections;
import javafx.collections.transformation.FilteredList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.input.MouseButton;
import javafx.scene.input.MouseEvent;
import org.apache.kafka.clients.admin.AdminClient;
import org.example.App;
import org.example.controller.Database;
import org.example.models.Kafka;
import org.example.topic.Broker;
import org.example.topic.Topic;
import org.example.utility.Utils;

import java.io.IOException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Predicate;

public class PrimaryController implements Initializable {

    @FXML
    TextField btServer;
    @FXML
    ListView<String> topics;
    @FXML
    ImageView searchImage;
    @FXML
    ImageView refreshImage;
    @FXML
    Menu connectionMenu;
    @FXML
    MenuBar menuBar;
    @FXML
    Label totalBrokers;
    @FXML
    Label totalTopics;
    @FXML
    Label totalPartitions;
    @FXML
    Menu connectionsItem;
    @FXML
    TextField search;

    Topic topic;
    List<String> uuidList;

    AdminClient adminClient;
    Properties config;
    ArrayList<String> list;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        config = getConfig();
        adminClient = AdminClient.create(config);
        topic = new Topic(adminClient);
        Set<String> topics = topic.getAllTopics();
        showTopic(topics);
        Image image = new Image("org/example/images/search.png");
        searchImage.setImage(image);
        Image refresh = new Image("org/example/images/ref.png");
        refreshImage.setImage(refresh);
        String partitions = String.valueOf(topic.getNumberOfPartitionsInCluster());
        totalPartitions.setText(partitions);
        String brokers = String.valueOf(new Broker(adminClient).getNumberOfBrokers());
        totalBrokers.setText(brokers);
        String topicCount = String.valueOf(topic.getTotalTopics(config));
        totalTopics.setText(topicCount);
    }

    private void showTopic(Set<String> topicList) {
        // System.out.println(topicList);
        list = new ArrayList<>(topicList);
        Collections.sort(list);
        topics.getItems().clear();
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
            String topic = topics.getSelectionModel().getSelectedItem();
            boolean isDeleted = this.topic.deleteTopic(topic);
            if (isDeleted) {
                System.out.println("deleted");
            }
        });
        cMenu.getItems().addAll(topicInfo, dataMenu, deleteMenu);
        topics.setContextMenu(cMenu);
    }

    @FXML
    private void refreshTopics() {
        Set<String> topics = topic.getAllTopics();
        showTopic(topics);
    }

    @FXML
    private void showConnections() {
        String query = "select * from CONNECTION";
        ResultSet rs = new Database().runQuery(query);
        uuidList = new ArrayList<>();
        Menu subMenu = new Menu();
        System.out.println("print connections");
        try {
            while (rs.next()) {
                MenuItem menuItem = new MenuItem(rs.getString("name"));
                uuidList.add(rs.getString("ID"));
                subMenu.getItems().add(menuItem);
            }
            connectionsItem.getItems().clear();
            connectionsItem.getItems().add(subMenu);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public Properties getConfig() {
        Utils utils = new Utils();
        Properties config;
        switch (Kafka.mechanism) {
            case "PLAINTEXT":
                config = utils.getKafkaProps();
                break;
            case "SSL":
                config = utils.getSSLProperties();
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + Kafka.mechanism);
        }
        return config;
    }

    @FXML
    private void filerTopics() {
        FilteredList<String> filteredData = new FilteredList<>(FXCollections.observableList(list));
        topics.setItems(filteredData);
        search.textProperty().addListener((observable, oldValue, newValue) ->
                filteredData.setPredicate(createPredicate(newValue))
        );
    }

    private Predicate<String> createPredicate(String searchText) {
        return topic -> {
            if (searchText == null || searchText.isEmpty()) return true;
            return findTopics(topic, searchText);
        };
    }

    private boolean findTopics(String topic, String searchText) {
        return (topic.toLowerCase().contains(searchText.toLowerCase()));
    }

}
