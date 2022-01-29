package org.example.controllers;

import javafx.animation.RotateTransition;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.shape.Circle;
import javafx.util.Duration;

import java.net.URL;
import java.util.ResourceBundle;

public class LoaderController implements Initializable {

    @FXML
    private Circle circle1;
    @FXML
    private Circle circle2;
    @FXML
    private Circle circle3;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        setRotate(circle1, true, 360, 10);
        setRotate(circle2, true, 180, 18);
        setRotate(circle3, true, 145, 24);

    }

    private void setRotate(Circle c, boolean reverse, int angle, int duration) {
        RotateTransition rotateTransition = new RotateTransition(Duration.seconds(duration), c);
        rotateTransition.setAutoReverse(reverse);
        rotateTransition.setByAngle(angle);
        rotateTransition.setDelay(Duration.seconds(0));
        rotateTransition.setRate(3);
        rotateTransition.setCycleCount(18);
        rotateTransition.play();
    }

}
