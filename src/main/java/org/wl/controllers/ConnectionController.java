package org.wl.controllers;

import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import org.wl.Alerts;
import org.wl.controller.Database;
import org.wl.models.Kafka;
import org.wl.utility.Utils;

import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class ConnectionController implements Initializable {

    @FXML
    private TextField connectionName;
    @FXML
    private TextField host;
    @FXML
    private TextField port;
    @FXML
    private TextField bootstrap;
    @FXML
    private Button save;
    @FXML
    private ListView<String> connectionList;
    @FXML
    private ChoiceBox<String> mechanism;
    @FXML
    private TextField ksLocation;
    @FXML
    private TextField ksPassword;
    @FXML
    private TextField tsLocation;
    @FXML
    private TextField tsPassword;
    @FXML
    private TextField keyPassword;
    @FXML
    private CheckBox checkbox;

    Database database;
    List<String> uuidList;
    int index;

    @FXML
    private void saveConnection() {
        String uuid = UUID.randomUUID().toString();

        String query = "Insert into CONNECTION VALUES ( '" + uuid + "', '"
                + connectionName.getText() + "' , '" + host.getText() + "' , '"
                + port.getText() + "' , '" + bootstrap.getText() + "' , '"
                + ksLocation.getText() + "' , '"
                + ksPassword.getText() + "' , '" + tsLocation.getText() + "' , '"
                + tsPassword.getText() + "' , '" + mechanism.getSelectionModel().getSelectedItem() + "' , '"
                + keyPassword.getText() + "' , '" + checkbox.isSelected()
                + "' )";
        if (database.insert(query) != 0)
            System.out.println("inserted in database");
        listConnections();
    }

    @FXML
    private void listConnections() {
        String query = "select * from CONNECTION";
        ResultSet rs = database.runQuery(query);
        ArrayList<String> list = new ArrayList<>();
        uuidList = new ArrayList<>();
        try {
            while (rs.next()) {
                list.add(rs.getString("NAME"));
                uuidList.add(rs.getString("ID"));
            }
            connectionList.getItems().clear();
            connectionList.getItems().addAll(list);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @FXML
    private void updateConnection() {
        String uuid = uuidList.get(index);
        String query = "UPDATE  CONNECTION SET ID = '" + uuid
                + "', name = '" + connectionName.getText() +
                "' , zookeeperHost = '" + host.getText()
                + "' , zookeeperPort = '" + port.getText()
                + "' , bootstrapServer = '" + bootstrap.getText()
                + "' , keystoreLocation = '" + ksLocation.getText()
                + "' , keyStorePassword = '" + ksPassword.getText()
                + "' , truststoreLocation = '" + tsLocation.getText()
                + "' , truststorePassword = '" + tsPassword.getText()
                + "' , mechanism = '" + mechanism.getSelectionModel().getSelectedItem()
                + "' , keyPassword = '" + keyPassword.getText()
                + "' , identificationAlgorithm = '" + checkbox.isSelected()
                + "' where id = '" + uuid + "'";
        if (database.insert(query) > 0)
            System.out.println("inserted in database");
        System.out.println("updated in database");


    }

    @FXML
    private void connect() {
        Kafka.bootstrapServer = bootstrap.getText();
        Kafka.ksLocation = ksLocation.getText();
        Kafka.ksPassword = ksPassword.getText();
        Kafka.tsLocation = tsLocation.getText();
        Kafka.tsPassword = tsPassword.getText();
        Kafka.mechanism = mechanism.getValue();
        Kafka.keyPassword = keyPassword.getText();
        Kafka.checkbox = checkbox.isSelected();
        new Utils().switchScene("primary");
    }

    @FXML
    private void showDetails() {
        index = connectionList.getSelectionModel().getSelectedIndices().get(0);
        String query = "select * from CONNECTION where id = '" + uuidList.get(index) + "'";
        ResultSet rs = database.runQuery(query);
        try {
            while (rs.next()) {
                connectionName.setText(rs.getString("name"));
                host.setText(rs.getString("zookeeperHost"));
                port.setText(rs.getString("zookeeperPort"));
                bootstrap.setText(rs.getString("bootstrapServer"));
                mechanism.setValue(rs.getString("mechanism"));
                if (!rs.getString("mechanism").equals("SSL")){
                    ksLocation.setText("");
                    ksPassword.setText("");
                    tsLocation.setText("");
                    tsPassword.setText("");
                    keyPassword.setText("");
                    checkbox.setSelected(false);
                }
                else {
                    ksLocation.setText(rs.getString("keystoreLocation"));
                    ksPassword.setText(rs.getString("keyStorePassword"));
                    tsLocation.setText(rs.getString("truststoreLocation"));
                    tsPassword.setText(rs.getString("truststorePassword"));
                    keyPassword.setText(rs.getString("keyPassword"));
                    checkbox.setSelected(rs.getBoolean("identificationAlgorithm"));
                }

            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @FXML
    private void clear() {
        connectionName.clear();
        host.clear();
        port.clear();
        bootstrap.clear();
        ksLocation.clear();
        ksPassword.clear();
        tsLocation.clear();
        tsPassword.clear();
        mechanism.setValue("PLAINTEXT");
        keyPassword.clear();
        checkbox.setSelected(false);
    }

    @FXML
    private void delete() {
        Alerts alert = new Alerts();
        if (alert.createConfirmation()) {
            int index = connectionList.getSelectionModel().getSelectedIndices().get(0);
            String query = "DELETE FROM CONNECTION where id = '" + uuidList.get(index) + "'";
            int rs = database.delete(query);
            if (rs != 0) {
                System.out.println("deleted");
            }
            listConnections();
        }

    }

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        database = new Database();
        listConnections();

    }
}
