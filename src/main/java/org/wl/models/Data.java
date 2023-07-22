package org.wl.models;

import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleLongProperty;
import javafx.beans.property.SimpleStringProperty;

public class Data {

    private SimpleStringProperty data;
    private SimpleLongProperty timestamp;
    private SimpleLongProperty offset;
    private SimpleIntegerProperty partition;

    public String getData() {
        return data.get();
    }

    public SimpleStringProperty dataProperty() {
        return data;
    }

    public void setData(String data) {
        this.data.set(data);
    }

    public Data(String data, long timestamp, long offset, int partition) {
        this.data = new SimpleStringProperty( data);
        this.timestamp = new SimpleLongProperty(timestamp);
        this.offset = new SimpleLongProperty(offset);
        this.partition = new SimpleIntegerProperty(partition);
    }

    public long getTimestamp() {
        return timestamp.get();
    }

    public SimpleLongProperty timestampProperty() {
        return timestamp;
    }

    public long getOffset() {
        return offset.get();
    }

    public SimpleLongProperty offsetProperty() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset.set(offset);
    }

    public int getPartition() {
        return partition.get();
    }

    public SimpleIntegerProperty partitionProperty() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition.set(partition);
    }


    public void setTimestamp(long timestamp) {
        this.timestamp.set(timestamp);
    }

}
