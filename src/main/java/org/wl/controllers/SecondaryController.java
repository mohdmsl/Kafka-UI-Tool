package org.wl.controllers;

import com.fasterxml.jackson.databind.ObjectMapper;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.PieChart;
import javafx.scene.chart.XYChart;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.input.KeyEvent;
import javafx.scene.layout.BorderPane;
import javafx.stage.Stage;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.json.XML;
import org.wl.Alerts;
import org.wl.App;
import org.wl.consumer.Consumer;
import org.wl.models.Data;
import org.wl.models.Kafka;
import org.wl.services.kafka.Topic;
import org.wl.utility.Utils;

import java.io.IOException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.ResourceBundle;
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
    @FXML
    BorderPane mainPane;
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
        new Utils().switchScene("secondary");
    }

    @FXML
    private void loadData() {

        Task<Parent> createMainScene = new Task<Parent>() {
            @Override
            public Parent call() {
                Parent root = null;

                return root;
            }
        };


        Stage loadingStage = new Stage();
        loadScreen(loadingStage);
        createMainScene.setOnSucceeded(e -> {
            mainPane.setDisable(false);
            loadingStage.close();
        });

        new Thread(createMainScene).start();


        table.getItems().clear();
        //  table.setItems( FXCollections.observableArrayList());
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

    /**
     * Downloads data to a file from kafka topic
     */
    @FXML
    private void downloadData() {
        Scene scene = null;
        try {
            FXMLLoader fxmlLoader = new FXMLLoader(App.class.getResource("downloadData.fxml"));
            scene = new Scene((Parent) fxmlLoader.load());
            Stage stage = new Stage();
            stage.setTitle("EXPORT DATA");
            stage.setScene(scene);
            stage.show();
        } catch (IOException e) {
            e.printStackTrace();
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
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(new Utils().getConfig());
        int partitionsCount = new Topic(adminClient).getNumberOfPartitions(Kafka.topicName, consumer);
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

    private void loadScreen(Stage stage) {
        mainPane.setDisable(true);
        Utils utils = new Utils();
        utils.createNewScene("loader", stage);

    }

}