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
            //init consumer
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
            //init offsets
            offsets.put(queue, new HashMap<>());
            for (String topic: topics) {
                offsets.put(topic, new HashMap<>());
            }
            for (Map<String, Integer> map: offsets.values()) {
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
                        break;
                    }
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
                    if(!message.headers().getString("MessageId").equals("asd")
                            || !message.properties().getString("PRO_OFFSET").equals("PRODUCER7_84")){
                        System.err.println("验证出错");
                        System.exit(-1);
                    }

//                    if(COUNT.incrementAndGet() % 100000 == 0){
//                    	System.out.println(COUNT.get());
//                    }
//                    System.out.println(Thread.currentThread().getName() +" " +queueOrTopic+" "+body);
//                    System.out.println(Thread.currentThread().getName() +" " + offsets.get(queueOrTopic).get(producer));


                    if (offset != offsets.get(queueOrTopic).get(producer)) {
                        logger.error("Mark Offset not equal expected:{} actual:{} producer:{} queueOrTopic:{}, thread : {}",
                                offsets.get(queueOrTopic).get(producer), offset, producer, queueOrTopic,Thread.currentThread().getName() );

                        break;
                    }

                    offsets.get(queueOrTopic).put(producer, offset + 1);
                    pullNum++;

                } catch (Exception e) {
                    logger.error("Error occurred in the consuming process", e);
                    break;
                }
            }
        }

        public int getPullNum() {
            return pullNum;
        }

    }

    public static void main(String[] args) throws Exception {
//    	 BasicConfigurator.configure();// 自动快速地使用缺省Log4j环境。
        System.out.println("start................");
        long start = System.currentTimeMillis();

        Random random = new Random();
        Thread[] ts = new Thread[Constants.CON_NUM];
        for (int i = 0; i < ts.length; i++) {
            String queue = Constants.QUEUE_PRE + i;
            List<String> topicLits = new ArrayList<>();
            Set<String> set = new HashSet<>();
            for(int len = 0;len <9 ;len++){
                String topic  = Constants.TOPIC_PRE+random.nextInt(5);
                while(set.contains(topic)){
                    topic  = Constants.TOPIC_PRE+random.nextInt(5);
                }
                set.add(topic);
                topicLits.add(topic);
            }

            ts[i] = new ConsumerTask(queue, topicLits);
        }

        for (int i = 0; i < ts.length; i++) {
            ts[i].start();
        }
        for (int i = 0; i < ts.length; i++) {
            ts[i].join();
        }
        int pullNum = 0;
        for (int i = 0; i < ts.length; i++) {
            pullNum += ((ConsumerTask)ts[i]).getPullNum();
            System.out.println(pullNum);
        }
        long end = System.currentTimeMillis();
        System.out.println("end................");
        System.out.println(String.format("Mark Consumer Finished, Cost %d ms, pullNum %d", end - start, pullNum));
        System.out.println(String.format("Cost %d q/ms", pullNum/(end - start)));
        System.exit(0);
    }
}
