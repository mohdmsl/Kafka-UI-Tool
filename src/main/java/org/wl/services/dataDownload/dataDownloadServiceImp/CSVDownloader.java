package org.wl.services.dataDownload.dataDownloadServiceImp;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.wl.services.dataDownload.dataDownloadService.DataDownloader;
import org.wl.services.kafka.Offsets;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CSVDownloader implements DataDownloader {

    private char separator = ',';
    private boolean withHeader = true;
    String mode = "append";
    public CSVDownloader() {
    }

    public CSVDownloader(char separator, boolean withHeader, String mode) {
        this.separator = separator;
        this.withHeader = withHeader;
        this.mode = mode;
    }

    public char getSeparator() {
        return separator;
    }

    public void setSeparator(char separator) {
        this.separator = separator;
    }

    public boolean isWithHeader() {
        return withHeader;
    }

    public void setWithHeader(boolean withHeader) {
        this.withHeader = withHeader;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    @Override
    public void downloadJsonData(KafkaConsumer<String, String> kafkaConsumer, PrintWriter printWriter,
                                 Offsets offsets, AtomicLong processedCount) {

        Map<Integer, Long> offsetsRead = new HashMap<>();
        for (Map.Entry<Integer, Long> entry : offsets.getStartOffset().entrySet()) {
            offsetsRead.put(entry.getKey(), entry.getValue() - 1);
        }

        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                records.forEach(record -> {
                    JSONObject json = new JSONObject(record.value());

                    StringBuffer header = new StringBuffer();
                    Iterator<String> headItr = json.keys();
                    if (this.withHeader) {
                        while (headItr.hasNext()) {
                            header.append(headItr.next());
                            header.append(this.separator);
                        }
                        setWithHeader(false);
                        header.deleteCharAt(header.length() - 1);
                        header.append("\n");
                        printWriter.append(header);
                    }

                    StringBuffer str = new StringBuffer();
                    Iterator<String> iterator = json.keys();
                    while (iterator.hasNext()) {
                        str.append(json.get(iterator.next()).toString());
                        str.append(this.separator);
                    }
                    str.deleteCharAt(str.length() - 1);
                    str.append("\n");
                    printWriter.append(str);
                    processedCount.getAndIncrement();
                    offsetsRead.put(record.partition(), record.offset());

                });
            } else {
                boolean allRead = true;
                for (Map.Entry<Integer, Long> entry : offsets.getEndOffset().entrySet()) {
                    allRead = offsetsRead.getOrDefault(entry.getKey(), -1L).equals(entry.getValue() - 1);
                    if (!allRead) break;
                }
                if (allRead) break;
            }
        }
    }
}
