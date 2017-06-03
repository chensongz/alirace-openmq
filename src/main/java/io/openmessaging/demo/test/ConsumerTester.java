package io.openmessaging.demo.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.MessageHeader;
import io.openmessaging.PullConsumer;

public class ConsumerTester {

    private static final AtomicLong COUNT = new AtomicLong(0);
    static Logger logger = LoggerFactory.getLogger(ConsumerTester.class);

    static class ConsumerTask extends Thread {
        String queue;
        List<String> topics;
        PullConsumer consumer;
        int pullNum;
        Map<String, Map<String, Integer>> offsets = new HashMap<>();

        public ConsumerTask(String queue, List<String> topics) {
            this.queue = queue;
            this.topics = topics;
            init();
        }

        public void init() {
            try {
                Class<?> kvClass = Class.forName("io.openmessaging.demo.DefaultKeyValue");
                KeyValue keyValue = (KeyValue) kvClass.newInstance();
                keyValue.put("STORE_PATH", Constants.STORE_PATH);
                Class<?> consumerClass = Class.forName("io.openmessaging.demo.DefaultPullConsumer");
                consumer = (PullConsumer) consumerClass.getConstructor(new Class[]{KeyValue.class}).newInstance(new Object[]{keyValue});
                if (consumer == null) {
                    throw new InstantiationException("Init Producer Failed");
                }
                consumer.attachQueue(queue, topics);
            } catch (Exception e) {
                logger.error("please check the package name and class name:", e);
            }
            offsets.put(queue, new HashMap<>());
            for (String topic : topics) {
                offsets.put(topic, new HashMap<>());
            }
            for (Map<String, Integer> map : offsets.values()) {
                for (int i = 0; i < Constants.PRO_NUM; i++) {
                    map.put(Constants.PRO_PRE + i, 0);
                }
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    BytesMessage message = (BytesMessage) consumer.poll();
                    if (message == null) {
//                        System.out.println("message is null!! consumer exit!!");
                        break;
                    }
//                    System.out.println(this.toString() + ": " + message.toString());
                    String queueOrTopic;
                    if (message.headers().getString(MessageHeader.QUEUE) != null) {
                        queueOrTopic = message.headers().getString(MessageHeader.QUEUE);
                    } else {
                        queueOrTopic = message.headers().getString(MessageHeader.TOPIC);
                    }
                    if (queueOrTopic == null || queueOrTopic.length() == 0) {
                        throw new Exception("Queue or Topic name is empty");
                    }
                    String body = new String(message.getBody());
                    int index = body.lastIndexOf("_");
                    String producer = body.substring(0, index);
                    int offset = Integer.parseInt(body.substring(index + 1));
                    if (!message.headers().getString("MessageId").equals("hfgdfgasdf")
                            || !message.properties().getString("PRO_OFFSET").equals("PRODUCER7_3")) {
                        System.out.println("header: " + message.headers().getString("MessageId") + "\nprop: " + message.properties().getString("PRO_OFFSET"));
                        System.err.println("验证出错");
                        break;
//                        System.exit(-1);
                    } else {
//                        System.out.println("验证成功");
                    }
//                    if(COUNT.incrementAndGet() % 100000 == 0){
//                    	System.out.println(COUNT.get());
//                    }
//                    System.out.println(Thread.currentThread().getName() +" " +queueOrTopic+" "+body);
//                    System.out.println(Thread.currentThread().getName() +" " + offsets.get(queueOrTopic).get(producer));
                    if (offset != offsets.get(queueOrTopic).get(producer)) {
                        System.err.printf("Mark Offset not equal expected:%s actual:%s producer:%s queueOrTopic:%s, thread : %s",
                                offsets.get(queueOrTopic).get(producer), offset, producer, queueOrTopic, Thread.currentThread().getName());
                        break;
                    }
                    offsets.get(queueOrTopic).put(producer, offset + 1);
                    pullNum++;
                } catch (Exception e) {
                    System.err.println("Error occurred in the consuming process!!");
                    e.printStackTrace();
                    break;
                }
            }
        }

        public int getPullNum() {
            return pullNum;
        }

        public List<String> getTopics() {
            return topics;
        }

    }

    public static void main(String[] args) throws Exception {
        System.out.println("start................");
        long start = System.currentTimeMillis();
        Random random = new Random();
        Thread[] ts = new Thread[Constants.CON_NUM];
        System.out.println("consumer_num: " + ts.length);
        for (int i = 0; i < ts.length; i++) {
            String queue = Constants.QUEUE_PRE + i;
            List<String> topicLits = new ArrayList<>();
            Set<String> set = new HashSet<>();
            for (int len = 0; len < Constants.TOPIC_NUM; len++) {
                String topic = Constants.TOPIC_PRE + random.nextInt(Constants.TOPIC_NUM);
                if (!set.contains(topic)) {
                    topicLits.add(topic);
                    if (topicLits.size() >= Constants.CON_NUM - 1) {
                        break;
                    }
                }
                set.add(topic);
            }
            ts[i] = new ConsumerTask(queue, topicLits);
//            System.out.println("consumer " + i + " attach queue: " + queue + " topics: " + topicLits);
        }
        for (Thread t : ts) {
            t.start();
        }
        for (Thread t : ts) {
            t.join();
        }
        long end = System.currentTimeMillis();
        System.out.println("end..................");

        int pullNum = 0;
        int shouldNum, actualNum;
        int successNum = 0, errorNum = 0;
        for (int i = 0; i < ts.length; i++) {
            actualNum = ((ConsumerTask) ts[i]).getPullNum();
            shouldNum = (Constants.PRO_NUM * Constants.PRO_MAX / (Constants.QUEUE_NUM + Constants.TOPIC_NUM))
                    * (((ConsumerTask) ts[i]).getTopics().size() + 1);
            pullNum += actualNum;
            System.out.println("consumer " + i
                    + " should pullNum " + shouldNum
                    + " actual pullNum " + actualNum
                    + (shouldNum == actualNum ? " success!!" : " error!!"));
            if (shouldNum == actualNum) {
                successNum++;
            } else {
                errorNum++;
            }
        }
        System.out.println("Consumer successNum: " + successNum + " errorNum: " + errorNum);
        System.out.println("Consume " + (errorNum == 0 ? "Success!!" : "Error!!"));
        System.out.println(String.format("Consumer Cost %d ms, total pullNum %d", end - start, pullNum));
        System.out.println(String.format("Tps %d qps", pullNum / (end - start)));
        System.exit(0);
    }
}
