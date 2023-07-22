package org.wl.models;

import javafx.concurrent.Task;

import java.util.concurrent.atomic.AtomicLong;

public class ProgressCounter extends Task<Long> {
    private long totalCount;
    private AtomicLong progressCount;

    public ProgressCounter(long totalCount, AtomicLong progressCount) {
        this.totalCount = totalCount;
        this.progressCount = progressCount;
    }

    @Override
    public Long call() {
        updateMessage("Starting");
        updateProgress(0, totalCount);

        while (progressCount.longValue() < totalCount) {
            System.out.println(progressCount.longValue());
            updateValue(progressCount.longValue());
            updateMessage(Long.toString(progressCount.longValue()));
            updateProgress(progressCount.longValue(), totalCount);
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        updateProgress(progressCount.longValue(), totalCount);
        // Return the result of the task
        return progressCount.longValue();
    }
}
