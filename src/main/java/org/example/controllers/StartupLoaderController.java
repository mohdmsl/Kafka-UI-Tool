package org.example.controllers;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.ProgressBar;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.Pane;
import javafx.stage.Stage;
import org.example.App;
import scala.math.Integral;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.util.ResourceBundle;

public class StartupLoaderController implements Initializable {

    @FXML
    private ProgressBar progressBar;

    @FXML
    private Pane pane;

    @FXML
    private ImageView backgroundImage;
    @FXML
    private Label percentLabel;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        try {
            FileInputStream inputStream = new FileInputStream("src/main/resources/org/example/images/background.jpg");
            Image image = new Image(inputStream);
            backgroundImage.setImage(image);
            new SplashScreen().start();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    private void loadProgress() {
        try {
            progressBar.setStyle("-fx-accent: #7CFC00");
            BigDecimal progress = new BigDecimal(String.format("%.2f", 0.0));
            for (int i = 1; i <= 100; i++) {
                progress = new BigDecimal(String.format("%.2f", progress.doubleValue() + 0.01));
                //System.out.println(progress.doubleValue());
                progressBar.setProgress(progress.doubleValue());
                //TODO: show percentage completed in label
                //percentLabel.setText(i + "%");
                Thread.sleep(50);
            }
            progress = null;
        } catch (Exception e) {

        }

    }

    class SplashScreen extends Thread {
        @Override
        public void run() {
            try {
                loadProgress();
                Platform.runLater(new Runnable() {
                    @Override
                    public void run() {
                        Stage stage = new Stage();
                        stage.setTitle("KafkaUI Tool");
                        Parent root = null;
                        try {
                            root = App.loadFXML("connection");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        Scene scene = new Scene(root);
                        stage.setScene(scene);
                        stage.show();
                        System.setProperty("APPLCATION_ID", "kafka_UI");
                        pane.getScene().getWindow().hide();
                    }
                });

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
