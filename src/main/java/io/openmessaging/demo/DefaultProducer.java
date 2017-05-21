package io.openmessaging.demo;

import io.openmessaging.*;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Map;

public class DefaultProducer  implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore;

    private KeyValue properties;

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        messageStore = new MessageStore(properties);
    }


    @Override public BytesMessage createBytesMessageToTopic(String topic, byte[] body) {
        return messageFactory.createBytesMessageToTopic(topic, body);
    }

    @Override public BytesMessage createBytesMessageToQueue(String queue, byte[] body) {
        return messageFactory.createBytesMessageToQueue(queue, body);
    }

    @Override public void start() {

    }

    @Override public void shutdown() {

    }

    @Override public KeyValue properties() {
        return properties;
    }

    @Override public void send(Message message) {
        messageStore.putMessage(message);
    }

    @Override public void send(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public Promise<Void> sendAsync(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void sendOneway(Message message, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public BatchToPartition createBatchToPartition(String partitionName, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void flush() {
//        String storePath = properties.getString("STORE_PATH");
//        String actualStorePath = storePath + "/" + this.toString();
//        Map<String, ArrayList<Message>> messageBuckets = messageStore.getMessageBuckets();
//        try {
//            PrintWriter pw = new PrintWriter(new FileWriter(actualStorePath), true);
//            for (String bucket: messageBuckets.keySet()) {
//                ArrayList<Message> bucketList = messageBuckets.get(bucket);
//                for (Message message : bucketList) {
//                    pw.println(message.toString());
//                }
//            }
//            pw.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
