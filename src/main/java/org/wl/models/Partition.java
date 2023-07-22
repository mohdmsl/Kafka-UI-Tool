package org.wl.models;

import javafx.beans.property.SimpleIntegerProperty;
import javafx.beans.property.SimpleLongProperty;

public class Partition {

    private SimpleIntegerProperty partition;
    private SimpleLongProperty startOffset;
    private SimpleLongProperty endOffset;
    private SimpleLongProperty records;

    public Partition(int partition, long startOffset, long endOffset, long records) {
        this.partition =  new SimpleIntegerProperty(partition);
        this.startOffset = new SimpleLongProperty(startOffset);
        this.endOffset = new SimpleLongProperty(endOffset);
        this.records = new SimpleLongProperty(records);
    }

    public long getStartOffset() {
        return startOffset.get();
    }

    public SimpleLongProperty startOffsetProperty() {
        return startOffset;
    }

    public void setStartOffset(long startOffset) {
        this.startOffset.set(startOffset);
    }

    public long getEndOffset() {
        return endOffset.get();
    }

    public SimpleLongProperty endOffsetProperty() {
        return endOffset;
    }

    public void setEndOffset(long endOffset) {
        this.endOffset.set(endOffset);
    }

    public long getRecords() {
        return records.get();
    }

    public SimpleLongProperty recordsProperty() {
        return records;
    }

    public void setRecords(int records) {
        this.records.set(records);
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
}
