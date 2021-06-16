package org.example;

import javafx.fxml.FXML;
import javafx.scene.control.Alert;
import javafx.scene.control.TextArea;
import javafx.scene.input.MouseEvent;
import javafx.scene.layout.Pane;

public class Alerts {

    @FXML
    Pane alertBox;

    @FXML
    private void close(MouseEvent mouseEvent){
    }


    private void createWarning(){

    }
    public void createInformation(Exception exception, String message){
        Alert aLert = new Alert(javafx.scene.control.Alert.AlertType.INFORMATION);
        aLert.setTitle("Alert");
        exception.printStackTrace();
        aLert.setContentText("Exception:" + message);

        TextArea area = new TextArea(exception.getMessage());
        aLert.getDialogPane().setExpandableContent(area);
        aLert.showAndWait();
    }

    private void createError(){

    }

    private void createConfirmation(){

    }
}
