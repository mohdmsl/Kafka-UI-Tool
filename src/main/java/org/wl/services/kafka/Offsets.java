package org.wl.services.kafka;

import java.util.Map;

public class Offsets {
    public Offsets(Map<Integer, Long> startOffset, Map<Integer, Long> endOffset) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    private Map<Integer, Long> startOffset;
    private Map<Integer, Long> endOffset;

    public Map<Integer, Long> getStartOffset() {
        return startOffset;
    }

    public void setStartOffset(Map<Integer, Long> startOffset) {
        this.startOffset = startOffset;
    }

    public Map<Integer, Long> getEndOffset() {
        return endOffset;
    }

    public void setEndOffset(Map<Integer, Long> endOffset) {
        this.endOffset = endOffset;
    }


}
