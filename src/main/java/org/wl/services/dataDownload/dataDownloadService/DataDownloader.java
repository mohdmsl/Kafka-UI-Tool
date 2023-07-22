package org.wl.services.dataDownload.dataDownloadService;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.wl.services.kafka.Offsets;

import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Salik Lari
 */
public interface DataDownloader {

    public void downloadJsonData(KafkaConsumer<String,String> kafkaConsumer, PrintWriter printWriter, Offsets offsets, AtomicLong processedCount);


}
