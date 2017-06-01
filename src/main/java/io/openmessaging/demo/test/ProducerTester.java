package io.openmessaging.demo.test;

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
                    DefaultBytesMessage message = (DefaultBytesMessage) producer.createBytesMessageToQueue(queueOrTopic, (label + "_" + offsets.get(queueOrTopic)).getBytes());
                    message.putHeaders("MessageId", "asd");

                    message.putProperties("inject_jig", "sdd");
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
        System.out.println(String.format("Mark Produce Finished, Cost %d ms, sendNum %d", end - start,Constants.PRO_MAX * 10));
        System.out.println(String.format("Cost %d q/ms", Constants.PRO_MAX * 10/(end - start)));
        System.exit(0);
    }
}
