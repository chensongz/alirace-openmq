package io.openmessaging.demo.test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.openmessaging.KeyValue;
import io.openmessaging.Producer;
import io.openmessaging.demo.DefaultBytesMessage;

public class ProducerTester {

    static class ProducerTask extends Thread {
        String label = Thread.currentThread().getName();
        Producer producer = null;
        int sendNum = 0;
        Map<String, Integer> offsets = new HashMap<>();
        List<String> TOPICS = new ArrayList<>();

        public ProducerTask(String label) {
            this.label = label;
            init();
        }

        public void init() {
            //init producer
            try {
                Class<?> kvClass = Class.forName("io.openmessaging.demo.DefaultKeyValue");
                KeyValue keyValue = (KeyValue) kvClass.newInstance();
                keyValue.put("STORE_PATH", Constants.STORE_PATH);
                Class<?> producerClass = Class.forName("io.openmessaging.demo.DefaultProducer");
                producer = (Producer) producerClass.getConstructor(new Class[]{KeyValue.class}).newInstance(new Object[]{keyValue});
                if (producer == null) {
                    throw new InstantiationException("Init Producer Failed");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            //init offsets
            for (int i = 0; i < Constants.QUEUE_NUM; i++) {
                offsets.put("QUEUE_" + i, 0);
                TOPICS.add("QUEUE_" + i);
            }
            for (int i = 0; i < Constants.TOPIC_NUM; i++) {
                offsets.put("TOPIC_" + i, 0);
                TOPICS.add("TOPIC_" + i);
            }

        }

        @Override
        public void run() {
            while (true) {
                try {
                    String queueOrTopic;
                    int index = sendNum % (Constants.TOPIC_NUM + Constants.QUEUE_NUM);
                    queueOrTopic = TOPICS.get(index);
//                    System.out.println(label + "_" + offsets.get(queueOrTopic));
                    DefaultBytesMessage message;
                    if (index < Constants.QUEUE_NUM) {
                        message = (DefaultBytesMessage) producer.createBytesMessageToQueue(queueOrTopic, (label + "_" + offsets.get(queueOrTopic)).getBytes());
                    } else {
                        message = (DefaultBytesMessage) producer.createBytesMessageToTopic(queueOrTopic, (label + "_" + offsets.get(queueOrTopic)).getBytes());

                    }
                    message.putHeaders("MessageId", "hfgdfgasdf");
                    message.putProperties("other_key", "uisfasdhf");
                    message.putProperties("PRO_OFFSET", "PRODUCER7_3");
//                    message.putProperties("iect_4", "e3w3");
//                    message.putProperties("iect_2", "x2y");
                    offsets.put(queueOrTopic, offsets.get(queueOrTopic) + 1);
                    producer.send(message);
                    sendNum++;
                    if (sendNum >= Constants.PRO_MAX) break;
                } catch (Exception e) {
                    e.printStackTrace();
                    break;
                }
            }
            producer.flush();
        }
    }

    public static void main(String[] args) throws Exception {
        File storePathDir = new File(Constants.STORE_PATH);
        String[] filenames = storePathDir.list();
        if (filenames != null && filenames.length != 0) {
            System.out.println("Remove old files...");
            for (String filename : filenames) {
                System.out.println("filename: " + filename);
                (new File(storePathDir + "/" + filename)).deleteOnExit();
            }
        }

        System.out.println("start................");
        long start = System.currentTimeMillis();
        Thread[] ts = new Thread[Constants.PRO_NUM];
        for (int i = 0; i < ts.length; i++) {
            ts[i] = new ProducerTask(Constants.PRO_PRE + i);
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].start();
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].join();
        }
        long end = System.currentTimeMillis();
        System.out.println("end................");
        System.out.println(String.format("Produce Finished, Cost %d ms, total sendNum %d", end - start, Constants.PRO_MAX * Constants.PRO_NUM));
        System.out.println(String.format("Tps %d qps", Constants.PRO_MAX * Constants.PRO_NUM / (end - start)));
        System.exit(0);
    }
}
