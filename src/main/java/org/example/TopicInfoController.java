package org.example;

import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.example.consumer.Consumer;
import org.example.models.Kafka;
import org.example.models.Partition;
import org.example.topic.TopicUtil;
import org.example.utility.Utils;

import java.net.URL;
import java.util.*;

public class TopicInfoController implements Initializable {

    @FXML
    Label noRecords;
    @FXML
    TableView<Partition> partitionTable;

    @FXML
    TableColumn<Partition, Integer> partition;
    @FXML
    TableColumn<Partition, Long> startOffset;
    @FXML
    TableColumn<Partition, Long> endOffset;
    @FXML
    TableColumn<Partition, Long> totalRecords;

    @Override
    public void initialize(URL url, ResourceBundle resourceBundle) {
        getTotalMessages();
    }

    @FXML
    public void getTotalMessages() {
        Properties config = new Utils().getKafkaProps();
        KafkaConsumer<String, String> kafkaConsumer = new Consumer().getConsumer(Kafka.topicName);
        TopicUtil tUtil = new TopicUtil();
        int partitionSize = tUtil.getNumberOfPartitions(Kafka.topicName, config);
        List<TopicPartition> partitions = new ArrayList<>();
        while (--partitionSize >= 0) {
            partitions.add(new TopicPartition(Kafka.topicName, partitionSize));
        }
        Map<TopicPartition, Long> map = kafkaConsumer.endOffsets(partitions);
        long size = 0l;
        for (Long l : map.values()) {
            size += l;
        }

        noRecords.setText(String.valueOf(size));
    }

    @FXML
    private void getPartitionDetails() {
        partition.setCellValueFactory(new PropertyValueFactory<Partition, Integer>("partition"));
        startOffset.setCellValueFactory(new PropertyValueFactory<Partition, Long>("startOffset"));
        endOffset.setCellValueFactory(new PropertyValueFactory<Partition, Long>("endOffset"));
        totalRecords.setCellValueFactory(new PropertyValueFactory<Partition, Long>("records"));
        ObservableList<Partition> partitionList = FXCollections.observableArrayList();

        Properties config = new Utils().getKafkaProps();
        KafkaConsumer<String, String> kafkaConsumer = new Consumer().getConsumer(Kafka.topicName);
        TopicUtil tUtil = new TopicUtil();
        int partitionSize = tUtil.getNumberOfPartitions(Kafka.topicName, config);
        List<TopicPartition> partitions = new ArrayList<>();
        for (int i = 0; i < partitionSize; i++) {
            partitions.add(new TopicPartition(Kafka.topicName, i));
        }
        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(partitions);
        Map<TopicPartition, Long> startOffsets = kafkaConsumer.beginningOffsets(partitions);
        List<Long> list1 = new ArrayList<>(endOffsets.values());
        List<Long> list2 = new ArrayList<>(startOffsets.values());
        for (int i = 0; i < partitionSize; i++) {
            long startOffset = list2.get(i);
            long endOffset = list1.get(i);
            Partition partition = new Partition(i, startOffset, endOffset, endOffset - startOffset);
            partitionList.add(partition);
        }

        partitionTable.setItems(partitionList);


    }
}
