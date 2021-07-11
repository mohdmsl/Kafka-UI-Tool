package org.example.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.layout.Pane;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.example.Alerts;
import org.example.models.Kafka;
import org.example.producer.Producer;
import org.example.utility.Utils;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.*;

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
    String button ;

    @FXML
    private void uploadJSON() {
        file = new FileChooser().showOpenDialog(new Stage());
        button= "json";
    }

    @FXML
    private void uploadCSV() {
        file = new FileChooser().showOpenDialog(new Stage());
        button= "csv";
    }

    @FXML
    private void upload() {
        try {
            JSONArray jsonArray = null;
            if (button.equals("json")) {
                JSONParser jsonParser = new JSONParser();
                jsonArray = (JSONArray) jsonParser.parse(new FileReader(file));
            } else {
                CsvSchema csvSchema = CsvSchema.builder().setUseHeader(true).build();
                CsvMapper csvMapper = new CsvMapper();
                List<Object> readAll = csvMapper.readerFor(Map.class).with(csvSchema).readValues(file).readAll();
                ObjectMapper mapper = new ObjectMapper();
                String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(readAll);

                JSONParser jsonParser = new JSONParser();
                jsonArray = (JSONArray) jsonParser.parse(json);

            }
            sendJson(jsonArray);
        } catch (Exception e) {
            new Alerts().createInformation(e, "unable to read file");
        }
    }


    private void sendJson(JSONArray jsonArray) {
        try {
            if (file != null) {
                Iterator itr = jsonArray.iterator();
                Properties config = new Utils().getKafkaProps();
                Producer producer = new Producer();
                KafkaProducer<String, String> kafkaProducer = producer.getProducer(config);
                System.out.println("starting upload");
                while (itr.hasNext()) {
                    String msg = itr.next().toString();
                    System.out.println(msg);
                    producer.sendMessage(Kafka.topicName, msg, kafkaProducer);
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
