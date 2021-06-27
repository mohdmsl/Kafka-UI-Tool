package org.example.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.KeyEvent;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.example.Alerts;
import org.example.App;
import org.example.consumer.Consumer;
import org.example.models.Data;
import org.example.models.Kafka;
import org.example.utility.Utils;
import org.json.JSONObject;
import org.json.XML;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.time.Duration;
import java.util.Iterator;
import java.util.ResourceBundle;

public class SecondaryController implements Initializable {
    @FXML
    TableView<Data> table;
    @FXML
    TableColumn<Data, String> data;
    @FXML
    TableColumn<Data, Long> timestamp;
    @FXML
    TableColumn<Data, Long> offset;
    @FXML
    TableColumn<Data, Integer> partition;
    @FXML
    TextArea dataField;
    @FXML
    ChoiceBox<String> formatter;
    @FXML
    Button downloadBtn;
    @FXML
    ChoiceBox<Integer> noofmsgs;

    String record;
    AdminClient adminClient;

    @FXML
    private void switchToPrimary() throws IOException {
        App.setRoot("primary");
    }

    @FXML
    private void loadData() {
        Consumer consumer = new Consumer(adminClient);
        KafkaConsumer<String, String> kafkaConsumer = consumer.getConsumerFromStartOffset(Kafka.topicName);
        data.setCellValueFactory(new PropertyValueFactory<Data, String>("data"));
        timestamp.setCellValueFactory(new PropertyValueFactory<Data, Long>("timestamp"));
        partition.setCellValueFactory(new PropertyValueFactory<Data, Integer>("partition"));
        offset.setCellValueFactory(new PropertyValueFactory<Data, Long>("offset"));
        int count = 0, currentRecCount = 0;
        boolean isCompleted = false;
        int noOfRecordsToDisplay = noofmsgs.getSelectionModel().getSelectedItem();
        System.out.println(noOfRecordsToDisplay);
        ObservableList<Data> dataList = FXCollections.observableArrayList();
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                Iterator<ConsumerRecord<String, String>> itr = records.iterator();
                while (itr.hasNext()) {
                    if (currentRecCount == noOfRecordsToDisplay) {
                        isCompleted = true;
                        break;
                    } else {
                        ConsumerRecord<String, String> record = itr.next();
                        System.out.println(record.value());

                        Data data = new Data(record.value(), record.timestamp(), record.offset(), record.partition());
                        dataList.add(data);
                    }
                    currentRecCount++;
                }
                count = 0;
            } else {
                count++;
                if (count > 50)
                    break;
            }
            if (isCompleted)
                break;
        }
        kafkaConsumer.unsubscribe();
        kafkaConsumer.close();
        table.setItems(dataList);
    }

    @FXML
    private void displayRecord() {
        record = table.getSelectionModel().getSelectedItem().getData();
        dataField.setText(record);

    }

    @FXML
    private void displayRecordOnKeyPressed(KeyEvent keyEvent) {
        if (keyEvent.getCode().isArrowKey()) {
            record = table.getSelectionModel().getSelectedItem().getData();
            dataField.setText(record);
        }

    }

    @FXML
    private void recordFormatter() {
        formatter.getSelectionModel().selectedItemProperty().addListener(new ChangeListener<String>() {
            @Override
            public void changed(ObservableValue<? extends String> observableValue, String oldValue, String newValue) {
                try {
                    if (newValue.equals("XML")) {
                        JSONObject json = new JSONObject(record);
                        String xml = XML.toString(json);
                        dataField.setText(xml);
                    } else if (newValue.equals("Json")) {
                        ObjectMapper mapper = new ObjectMapper();
                        String json = mapper.readTree(record).toPrettyString();
                        dataField.setText(json);
                    } else {
                        dataField.setText(record);
                    }
                } catch (Exception e) {
                    new Alerts().createInformation(e, "unable to change format");
                }
            }
        });

    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        formatter.getItems().add("Json");
        formatter.getItems().add("Text");
        formatter.getItems().add("XML");
        formatter.setValue("Text");

        noofmsgs.getItems().addAll(10, 50, 100, 200, 300, 400, 500);
        noofmsgs.setValue(100);
        adminClient = AdminClient.create(new Utils().getKafkaProps());

    }

    @FXML
    private void downloadData() {
        File file = new FileChooser().showSaveDialog(new Stage());
        try {
            if (file != null) {
                Consumer consumer = new Consumer(adminClient);
                KafkaConsumer<String, String> kafkaConsumer = consumer.getConsumerFromStartOffset(Kafka.topicName);
                int count = 0;
                PrintWriter pw = new PrintWriter(file);
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                    if (!records.isEmpty()) {
                        records.forEach(record -> {
                            pw.append(record.value());
                        });
                        count = 0;
                    } else {
                        count++;
                        if (count > 3)
                            break;
                    }
                }
                kafkaConsumer.unsubscribe();
                kafkaConsumer.close();
            }
        } catch (Exception e) {
            new Alerts().createInformation(e, "unable to download");
        }
    }
}