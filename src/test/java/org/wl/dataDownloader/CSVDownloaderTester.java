package org.wl.dataDownloader;


import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.wl.consumer.Consumer;
import org.wl.services.dataDownload.dataDownloadServiceImp.CSVDownloader;
import org.wl.services.dataLoad.CSVFileLoader;
import org.wl.services.kafka.Offsets;
import org.wl.services.kafka.Topic;
import org.wl.utility.Utils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class CSVDownloaderTester {


    @Test
    @DisplayName("write csv file")
    public void writeCsvFileTest() {

        String bootstrap = "10.10.28.44:9391,10.10.28.44:9392,10.10.28.44:9393";
        Properties config = new Utils().getTestKafkaProps(bootstrap);

        AdminClient adminClient = AdminClient.create(config);
        String topicName = "ORacle_Upsert_event_1_1686632443013_6989";

        Consumer consumer = new Consumer(adminClient);
        KafkaConsumer<String, String> kafkaConsumer = consumer.getConsumerFromStartOffset(topicName, config);
        Topic topic = new Topic();
        Offsets offsets = topic.getOffsets(topicName, kafkaConsumer);
        AtomicLong processedCount = new AtomicLong();
        CSVDownloader downloader = new CSVDownloader();
        PrintWriter pw = null;

        try {
            FileWriter file = new FileWriter("src/test/resources/testFiles/test2.csv", true);
            /*if (file.exists())
                downloader.setWithHeader(false);*/

            pw = new PrintWriter(file);

            long total = 0;
            for (Map.Entry<Integer, Long> value : offsets.getEndOffset().entrySet()) {
                total = total + (value.getValue() - offsets.getStartOffset().get(value.getKey()));
            }
          /*  Counter counter = new Counter(processedCount, total);
            Thread thread = new Thread(counter);
            thread.start();*/
            downloader.downloadJsonData(kafkaConsumer, pw, offsets, processedCount);
            kafkaConsumer.unsubscribe();
            kafkaConsumer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (pw != null)
                pw.close();
        }
        long count = 0;
        try {

            // make a connection to the file
            Path outputFile = Paths.get("src/test/resources/testFiles/test2.csv");

            // read all lines of the file
            count = Files.lines(outputFile).count();
            System.out.println("Total Lines: " + count);

        } catch (Exception e) {
            e.getStackTrace();
        } finally {
            Assertions.assertEquals(1000, count);
        }
    }

    @Test
    @DisplayName("pipe delimited")
    public void testPipeDelimited() {
        CSVFileLoader loader = new CSVFileLoader();
        loader.setSeparator('|');
        List<String> list = loader.readCSVFileInJson(new File("src/test/resources/testFiles/dmPipe.csv"));
        Assertions.assertEquals(10, list.size());

    }

    @Test
    @DisplayName("Read csv file as text")
    public void readAsText() {
        CSVFileLoader loader = new CSVFileLoader();
        List<String> list = loader.readCSVFileAsText("src/test/resources/testFiles/dm.csv");
        Assertions.assertEquals(10, list.size());
    }


}
