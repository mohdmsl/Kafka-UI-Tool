package org.wl.services.dataDownload.dataDownloadServiceImp;

import javafx.concurrent.Task;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.wl.services.kafka.Offsets;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class JsonDownloader extends Task<Long> {

    private long totalCount;
    private KafkaConsumer<String, String> kafkaConsumer;
    private PrintWriter printWriter;
    private Offsets offsets;

    public JsonDownloader(long totalCount, KafkaConsumer<String, String> kafkaConsumer, PrintWriter printWriter, Offsets offsets) {
        this.totalCount = totalCount;
        this.kafkaConsumer = kafkaConsumer;
        this.printWriter = printWriter;
        this.offsets = offsets;
    }


    @Override
    protected Long call() throws Exception {
        Map<Integer, Long> offsetsRead = new HashMap<>();
        for (Map.Entry<Integer, Long> entry : offsets.getStartOffset().entrySet()) {
            offsetsRead.put(entry.getKey(), entry.getValue() - 1);
        }
        printWriter.append("[");
        boolean isFirstRecord = true;
        long progressCount = 0;
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, String> record : records) {
                        if (isFirstRecord) {
                            isFirstRecord = false;
                        } else {
                            printWriter.append(",");
                        }
                        printWriter.append(record.value());
                        printWriter.println();
                        offsetsRead.put(record.partition(), record.offset());
                        progressCount = progressCount + 1;

                    }
                    System.out.println(progressCount);
                    updateValue(progressCount);
                    updateProgress(progressCount, totalCount);
                } else {
                    boolean allRead = true;
                    for (Map.Entry<Integer, Long> entry : offsets.getEndOffset().entrySet()) {
                        allRead = offsetsRead.getOrDefault(entry.getKey(), -1L).equals(entry.getValue() - 1);
                        if (!allRead) break;
                    }
                    if (allRead) {
                        printWriter.append("]");
                        kafkaConsumer.unsubscribe();
                        kafkaConsumer.close();
                        this.cancel();
                        break;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (printWriter != null)
                printWriter.close();
        }
        return progressCount;
    }
}
