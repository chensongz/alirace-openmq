package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class MessageStore {

    private Map<String, LinkedList<Message>> messageBuckets = new HashMap<>();
    private Map<String, HashMap<String, Integer>> queueOffsets = new HashMap<>();

    private PrintWriter printWriter = null;
    public MessageStore(KeyValue properties)  {
        String storePath = properties.getString("STORE_PATH");
        String actualStorePath = storePath + "/" + this.toString();
        try {
            printWriter = new PrintWriter(new FileWriter(actualStorePath));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void putMessage(Message message) {
        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }
        if (printWriter != null) {
            printWriter.println(message.toString());
        }
    }

    public void transferMessage(Message message) {
        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }
        String bucket = topic != null ? topic : queue;
        // if messageBuckets don't contain specific topic or queue,
        // then add topic or queue to messageBuckets.
        if (!messageBuckets.containsKey(bucket)) {
            messageBuckets.put(bucket, new LinkedList<>());
        }
        LinkedList<Message> bucketList = messageBuckets.get(bucket);
        bucketList.add(message);
    }

    public Message pullMessage(String queue, String bucket) {
        LinkedList<Message> bucketList = messageBuckets.get(bucket);
        if (bucketList == null) {
            return null;
        }
        HashMap<String, Integer> offsetMap = queueOffsets.computeIfAbsent(queue, k -> new HashMap<>());
        int offset = offsetMap.getOrDefault(bucket, 0);
        if (offset >= bucketList.size()) {
            return null;
        }
        Message message = bucketList.get(offset);
        offsetMap.put(bucket, ++offset);
        return message;
    }


    public Map<String, LinkedList<Message>> getMessageBuckets() {
        return this.messageBuckets;
    }

    public void flush() {
        if (printWriter != null) {
            printWriter.close();
        }
    }
}
