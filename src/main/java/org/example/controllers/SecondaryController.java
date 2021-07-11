package org.example.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ListChangeListener;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.chart.*;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.HBox;
import javafx.scene.text.Text;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.example.Alerts;
import org.example.App;
import org.example.consumer.Consumer;
import org.example.models.Data;
import org.example.models.Kafka;
import org.example.topic.Offsets;
import org.example.topic.Topic;
import org.example.utility.Utils;
import org.json.JSONObject;
import org.json.XML;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

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
    @FXML
    LineChart lineChart;
    @FXML
    PieChart partitionChart;
    @FXML
    Button deleteButton;
/*    @FXML
    TitledPane titledPane;*/


    String textFormat = "Text";


    String record;
    AdminClient adminClient;
    ObservableList<Data> dataList = null;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        formatter.getItems().add("Json");
        formatter.getItems().add("Text");
        formatter.getItems().add("XML");
        formatter.setValue("Text");

        noofmsgs.getItems().addAll(10, 50, 100, 200, 300, 400, 500);
        noofmsgs.setValue(100);
        adminClient = AdminClient.create(new Utils().getKafkaProps());
        // titledPane.setText(Kafka.topicName);

    }

    @FXML
    private void switchToPrimary() throws IOException {
        App.setRoot("primary");
    }

    @FXML
    private void loadData() {
        table.getItems().clear();
        Consumer consumer = new Consumer(adminClient);
        KafkaConsumer<String, String> kafkaConsumer = consumer.getConsumerFromStartOffset(Kafka.topicName);
        data.setCellValueFactory(new PropertyValueFactory<Data, String>("data"));
        timestamp.setCellValueFactory(new PropertyValueFactory<Data, Long>("timestamp"));
        partition.setCellValueFactory(new PropertyValueFactory<Data, Integer>("partition"));
        offset.setCellValueFactory(new PropertyValueFactory<Data, Long>("offset"));
        dataList = getDataList(kafkaConsumer);
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
            try {
                if (textFormat.equals("XML")) {
                    JSONObject json = new JSONObject(record);
                    String xml = XML.toString(json);
                    dataField.setText(xml);
                } else if (textFormat.equals("Json")) {
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
                        textFormat = "XML";
                    } else if (newValue.equals("Json")) {
                        ObjectMapper mapper = new ObjectMapper();
                        String json = mapper.readTree(record).toPrettyString();
                        dataField.setText(json);
                        textFormat = "Json";
                    } else {
                        dataField.setText(record);
                        textFormat = "Text";
                    }
                } catch (Exception e) {
                    new Alerts().createInformation(e, "unable to change format");
                }
            }
        });

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
                        if (count > 50)
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

    @FXML
    private void deleteData() {
        Topic topic = new Topic(adminClient);
        boolean isDeleted = topic.deleteTopic(Kafka.topicName);
        if (isDeleted) {
            topic.createTopic(Kafka.topicName);
            System.out.println("created");
        }

    }

    @FXML
    private void drawCharts() {
        lineChart.getData().clear();

        if (dataList != null) {
            drawRecordsChart();
            drawOffsetsChart();
        } else {
            Consumer consumer = new Consumer(adminClient);
            KafkaConsumer<String, String> kafkaConsumer = consumer.getConsumerFromStartOffset(Kafka.topicName);
            dataList = getDataList(kafkaConsumer);
            drawRecordsChart();
            drawOffsetsChart();
        }
    }

    private ObservableList<Data> getDataList(KafkaConsumer<String, String> kafkaConsumer) {
        int count = 0, currentRecCount = 0;
        boolean isCompleted = false;
        int noOfRecordsToDisplay = Integer.MAX_VALUE;
        ObservableList<Data> oList = FXCollections.observableArrayList();
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
                        Data data = new Data(record.value(), record.timestamp(), record.offset(), record.partition());
                        oList.add(data);
                    }
                    currentRecCount++;
                }
                count = 0;
            } else {
                count++;
                if (count > 10)
                    break;
            }
            if (isCompleted)
                break;
        }
        kafkaConsumer.unsubscribe();
        kafkaConsumer.close();
        return oList;
    }

    /*private ObservableList<Data> getDataList() {
        int count = 0, recordsCount = 0;
        boolean isSuccessful = false;
        int noOfRecordsToDisplay = noofmsgs.getSelectionModel().getSelectedItem();
        ObservableList<Data> oList = FXCollections.observableArrayList();
        Consumer consumer = new Consumer(adminClient);
        Offsets offsets = new Topic(adminClient).getOffsets(Kafka.topicName);
        KafkaConsumer<String, String> kafkaConsumer = consumer.getConsumerFromStartOffset(Kafka.topicName);
        Map<TopicPartition, Long> endOffset = offsets.getEndOffset();
        Map<Integer, Long> offsetMap = new HashMap<>();
        Iterator<TopicPartition> itr1 = endOffset.keySet().iterator();
        while (itr1.hasNext()) {
            TopicPartition partition = itr1.next();
            offsetMap.put(partition.partition(), endOffset.get(partition));
        }

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                System.out.println("consuming");
                Iterator<ConsumerRecord<String, String>> itr = records.iterator();
                while (itr.hasNext()) {
                    ConsumerRecord<String, String> record = itr.next();
                    Data data = new Data(record.value(), record.timestamp(), record.offset(), record.partition());
                    oList.add(data);
                    if (record.offset() == offsetMap.get(record.partition())) {
                        if (++count == endOffset.size())
                            isSuccessful = true;
                        break;
                    }
                    if (++recordsCount == noOfRecordsToDisplay)
                        isSuccessful = true;
                    break;
                }
            }
            if (isSuccessful)
                break;
        }
        kafkaConsumer.unsubscribe();
        kafkaConsumer.close();
        return oList;

    }*/

    private void drawRecordsChart() {
        lineChart.getData().clear();
        XYChart.Series dataSeries1 = new XYChart.Series();
        Map<String, Long> mappedData = dataList.stream().collect(Collectors.groupingBy(data -> {
            Date date = new Date(data.getTimestamp());
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date).substring(12, 14);
        }, Collectors.counting()));
        Iterator itr = mappedData.keySet().iterator();
        while (itr.hasNext()) {
            String time = itr.next().toString();
            System.out.println(time);
            dataSeries1.getData().add(new XYChart.Data(time + "O'Clock", mappedData.get(time)));
        }
        lineChart.getData().add(dataSeries1);
    }

    private void drawOffsetsChart() {
        partitionChart.getData().clear();
//        XYChart.Series<NumberAxis, NumberAxis> dataSeries1 = new XYChart.Series<>();
//        dataSeries1.setName("Records per Offset");
        int partitionsCount = new Topic(adminClient).getNumberOfPartitions(Kafka.topicName, new Utils().getConfig());
        Map<Integer, Long> mappedData = dataList.stream().collect(Collectors.groupingBy(Data::getPartition, Collectors.counting()));
        ObservableList<PieChart.Data> pieChartData = FXCollections.observableArrayList();
        for (int i = 0; i < partitionsCount; i++) {
            if (mappedData.containsKey(i)) {
                // dataSeries1.getData().add(new XYChart.Data(String.valueOf(i), mappedData.get(i)));
                pieChartData.add(new PieChart.Data("Partition: " + i, mappedData.get(i)));
            } else {
                // dataSeries1.getData().add(new XYChart.Data(String.valueOf(i), 0));
                pieChartData.add(new PieChart.Data("Partition: " + i, 0));
            }
        }
        /*  bar chart representation with tooltip
        partitionChart.getData().add(dataSeries1);

        ObservableList<XYChart.Series> chartData = partitionChart.getData();
        chartData.forEach(x -> {
            ObservableList<XYChart.Data> ls = x.getData();
            ls.forEach(y -> {
                Tooltip.install(y.getNode(), new Tooltip(y.getYValue().toString() + " records"));
                y.getNode().setOnMouseEntered(event -> y.getNode().getStyleClass().add("onHover"));
                y.getNode().setOnMouseExited(event -> y.getNode().getStyleClass().remove("onHover"));
            });
        });
*/
        partitionChart.getData().addAll(pieChartData);

        ObservableList<PieChart.Data> chartData = partitionChart.getData();
        chartData.forEach(x -> {
            Tooltip.install(x.getNode(), new Tooltip(x.getPieValue() + " records"));
            x.getNode().setOnMouseEntered(event -> {
                x.getNode().getStyleClass().add("onHover");
            });
            x.getNode().setOnMouseExited(event -> x.getNode().getStyleClass().remove("onHover"));
        });
    }

    @FXML
    private void uploadData() {
        Scene scene = null;
        try {
            FXMLLoader fxmlLoader = new FXMLLoader(App.class.getResource("produceData.fxml"));
            scene = new Scene((Parent) fxmlLoader.load());
            Stage stage = new Stage();
            stage.setTitle("UPLOAD DATA");
            stage.setScene(scene);
            stage.show();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}