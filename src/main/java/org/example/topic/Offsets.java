package org.example.topic;

import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class Offsets {
    public Offsets(Map<TopicPartition, Long> startOffset, Map<TopicPartition, Long> endOffset) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    private Map<TopicPartition, Long> startOffset;
    private Map<TopicPartition, Long> endOffset;

    public Map<TopicPartition, Long> getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(Map<TopicPartition, Long> startOffset) {
        this.startOffset = startOffset;
    }

    public Map<TopicPartition, Long> getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(Map<TopicPartition, Long> endOffset) {
        this.endOffset = endOffset;
    }
}
