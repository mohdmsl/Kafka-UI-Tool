package org.wl.controllers;

import javafx.fxml.FXML;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressBar;
import javafx.scene.layout.Pane;
import javafx.stage.FileChooser;
import javafx.stage.Stage;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.wl.Alerts;
import org.wl.consumer.Consumer;
import org.wl.models.Kafka;
import org.wl.services.dataDownload.dataDownloadServiceImp.CSVDownloader;
import org.wl.services.dataDownload.dataDownloadServiceImp.JsonDownloader;
import org.wl.services.kafka.Offsets;
import org.wl.services.kafka.Topic;
import org.wl.utility.Utils;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Salik Lari
 */
public class DataDownloadController {
    @FXML
    private Button csvBtn;
    @FXML
    private Button jsonBtn;
    @FXML
    private Button cancelBtn;
    @FXML
    private Button saveButton;
    @FXML
    private Pane pane;

    @FXML
    private ProgressBar progressBar;

    @FXML
    private Button fileBtn;

    @FXML
    private Label totalRecordsLabel;

    @FXML
    private Label recordsProcessedLabel;

    private String fileType = "json";

    private File file = null;

    private JsonDownloader jsonDownloaderTask;

    @FXML
    private void initialize() {
        csvBtn.setOnAction(actionEvent -> {
            fileType = "csv";

            csvBtn.setStyle("--fx-border-color:#00A300");
            csvBtn.setStyle("-fx-border-width:2.5");

            //reset cv button border format
            jsonBtn.setStyle("--fx-border-color: #000000");
            jsonBtn.setStyle("-fx-border-width:0.5");
        });
        jsonBtn.setOnAction(actionEvent -> {
            fileType = "json";
            //change json border color
            jsonBtn.setStyle("--fx-border-color:#00A300");
            jsonBtn.setStyle("-fx-border-width:2.5");

            //reset cv button border format
            csvBtn.setStyle("--fx-border-color: #000000");
            jsonBtn.setStyle("-fx-border-width:0.5");
        });

    }

    @FXML
    private void saveFile() {
        if (saveButton.getText().toLowerCase().equals("save")) {
            Properties config = new Utils().getKafkaProps();
            AdminClient adminClient = AdminClient.create(config);
            try {
                if (file != null) {
                    saveButton.setDisable(true);
                    Consumer consumer = new Consumer(adminClient);
                    KafkaConsumer<String, String> kafkaConsumer = consumer.getConsumerFromStartOffset(Kafka.topicName);
                    Topic topic = new Topic();
                    Offsets offsets = topic.getOffsets(Kafka.topicName, kafkaConsumer);
                    AtomicLong processedCount = new AtomicLong();
                    PrintWriter pw = null;

                    try {
                        pw = new PrintWriter(file);
                        long total = 0;
                        for (Map.Entry<Integer, Long> value : offsets.getEndOffset().entrySet()) {
                            total = total + (value.getValue() - offsets.getStartOffset().get(value.getKey()));
                        }
                        totalRecordsLabel.setVisible(true);
                        totalRecordsLabel.setText(String.valueOf(total));
                        progressBar.setVisible(true);
                        progressBar.setStyle("-fx-accent: #7CFC00");
                        recordsProcessedLabel.setVisible(true);

                        saveFileToLocation(kafkaConsumer, pw, offsets, total, processedCount);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    showFile();
                }
            } catch (Exception e) {
                new Alerts().createInformation(e, "unable to download");
            }
        } else {
            String os = System.getProperty("os.name");
            if (os.toLowerCase().startsWith("win")) {
                try {
                    String command = "explorer /select, " + file.getAbsolutePath();
                    Runtime.getRuntime().exec(command);
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    public void saveFileToLocation(KafkaConsumer<String, String> kafkaConsumer, PrintWriter printWriter, Offsets offsets,
                                   long totalCount, AtomicLong progressCount) {
        try {
            if (fileType.equals("json")) {
                jsonDownloaderTask = new JsonDownloader(totalCount, kafkaConsumer, printWriter, offsets);
                jsonDownloaderTask.valueProperty().addListener((observable, oldValue, newValue) -> recordsProcessedLabel.setText(String.valueOf(newValue) +" / "));
                progressBar.progressProperty().bind(jsonDownloaderTask.progressProperty());

                Thread th = new Thread(jsonDownloaderTask);
                th.setDaemon(true);
                th.start();
            } else if (fileType.equals("csv")) {
                CSVDownloader loader = new CSVDownloader();
                loader.downloadJsonData(kafkaConsumer, printWriter, offsets, progressCount);
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
    }

    @FXML
    public void createFile() {
        FileChooser fileChooser = new FileChooser();
        String extension = "";
        if (fileType.equals("json")) {
            extension = "*.json";
        } else if (fileType.equals("csv")) {
            extension = "*.csv";
        }
        FileChooser.ExtensionFilter extFilter = new FileChooser.ExtensionFilter("JSON files ", extension);
        fileChooser.getExtensionFilters().add(extFilter);
        fileChooser.setInitialFileName(Kafka.topicName);
        file = fileChooser.showSaveDialog(new Stage());
        fileBtn.setText(file.getName());
    }

    public void showFile() {
        saveButton.setDisable(false);
        saveButton.setText("Show File");
    }

    @FXML
    private void close() {
        Stage stage = (Stage) pane.getScene().getWindow();
        stage.close();
    }


}
