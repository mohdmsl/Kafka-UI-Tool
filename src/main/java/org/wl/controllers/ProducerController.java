package org.wl.controllers;

import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.layout.Pane;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.wl.Alerts;
import org.wl.models.Kafka;
import org.wl.producer.Producer;
import org.wl.services.dataLoad.CSVFileLoader;
import org.wl.services.dataLoad.JsonFileLoader;
import org.wl.utility.Utils;

import java.io.File;
import java.util.List;
import java.util.Properties;

public class ProducerController {

    @FXML
    private Button csvBtn;
    @FXML
    private Button jsonBtn;
    @FXML
    private Button cancelBtn;
    @FXML
    private Button uploadBtn;
    @FXML
    private Pane pane;
    private File file = null;
    String button;

    @FXML
    private void uploadJSON() {
        file = new FileChooser().showOpenDialog(new Stage());
        button = "json";
    }

    @FXML
    private void uploadCSV() {
        file = new FileChooser().showOpenDialog(new Stage());
        button = "csv";
    }

    @FXML
    private void upload() {
        try {
            List<String> records = null;
            if (button.equals("json")) {
                JsonFileLoader loader = new JsonFileLoader();
                records = loader.readJsonFile(file);

            } else {
                CSVFileLoader loader = new CSVFileLoader();
                records = loader.readCSVFileInJson(file);

            }
            sendJson(records);
        } catch (Exception e) {
            new Alerts().createInformation(e, "unable to read file");
        }
    }


    private void sendJson(List<String> records) {
        try {
            if (file != null) {
                Properties config = new Utils().getKafkaProps();
                Producer producer = new Producer();
                KafkaProducer<String, String> kafkaProducer = producer.getProducer(config);
                System.out.println("starting upload");
                for (String rec : records) {
                    producer.sendMessage(Kafka.topicName, rec, kafkaProducer);
                }
                new Alerts().createSuccess("Upload Successful");
            }
        } catch (Exception e) {
            new Alerts().createInformation(e, "unable to upload file");
        }
    }

    @FXML
    private void close() {
        Stage stage = (Stage) pane.getScene().getWindow();
        stage.close();
    }
}
