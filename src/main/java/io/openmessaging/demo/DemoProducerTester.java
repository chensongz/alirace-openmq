package io.openmessaging.demo;

import io.openmessaging.*;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DemoProducerTester {
    public static final int BUCKET_MESSAGE_COUNT = 10240;

    public static void main(String[] args) {
        KeyValue properties = new DefaultKeyValue();
        /*
        //实际测试时利用 STORE_PATH 传入存储路径
        //所有producer和consumer的STORE_PATH都是一样的，选手可以自由在该路径下创建文件
         */
//        properties.put("STORE_PATH", "/home/zwy/test");
        properties.put("STORE_PATH", "/home/zhuchensong/project/aliyun/zbz/test");

        //这个测试程序的测试逻辑与实际评测相似，但注意这里是单线程的，实际测试时会是多线程的，并且发送完之后会Kill进程，再起消费逻辑

        Producer producer = new DefaultProducer(properties);

        //构造测试数据
        String topic1 = "TOPIC1"; //实际测试时大概会有100个Topic左右
        String topic2 = "TOPIC2"; //实际测试时大概会有100个Topic左右
        String queue1 = "QUEUE1"; //实际测试时，queue数目与消费线程数目相同
        String queue2 = "QUEUE2"; //实际测试时，queue数目与消费线程数目相同
        List<Message> messagesForTopic1 = new ArrayList<>(BUCKET_MESSAGE_COUNT);
        List<Message> messagesForTopic2 = new ArrayList<>(BUCKET_MESSAGE_COUNT);
        List<Message> messagesForQueue1 = new ArrayList<>(BUCKET_MESSAGE_COUNT);
        List<Message> messagesForQueue2 = new ArrayList<>(BUCKET_MESSAGE_COUNT);
        for (int i = 0; i < BUCKET_MESSAGE_COUNT; i++) {
            //注意实际比赛可能还会向消息的headers或者properties里面填充其它内容
            messagesForTopic1.add(producer.createBytesMessageToTopic(topic1, (topic1 + i).getBytes()));
            messagesForTopic2.add(producer.createBytesMessageToTopic(topic2, (topic2 + i).getBytes()));
            messagesForQueue1.add(producer.createBytesMessageToQueue(queue1, (queue1 + i).getBytes()));
            messagesForQueue2.add(producer.createBytesMessageToQueue(queue2, (queue2 + i).getBytes()));
        }

        long start = System.currentTimeMillis();
        //发送, 实际测试时，会用多线程来发送, 每个线程发送自己的Topic和Queue
        for (int i = 0; i < BUCKET_MESSAGE_COUNT; i++) {
            producer.send(messagesForTopic1.get(i));
            producer.send(messagesForTopic2.get(i));
            producer.send(messagesForQueue1.get(i));
            producer.send(messagesForQueue2.get(i));
        }
        producer.flush();
        long end = System.currentTimeMillis();

        long T1 = end - start;

        System.out.printf("SendNum: %d, SendCost: %d ms, SendTps: %d qps",
                BUCKET_MESSAGE_COUNT * 4,
                T1, (BUCKET_MESSAGE_COUNT * 4) / T1);

        //请保证数据写入磁盘中
    }
}
