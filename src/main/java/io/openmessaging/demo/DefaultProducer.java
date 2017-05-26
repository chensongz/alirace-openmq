package io.openmessaging.demo;

import io.openmessaging.*;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class DefaultProducer  implements Producer {
    private MessageFactory messageFactory = new DefaultMessageFactory();
    private MessageStore messageStore = MessageStore.getInstance();

    private KeyValue properties;

    private String storePath;
    private Map<String, PrintWriter> printWriterHashMap = new HashMap<>(1024);

    public DefaultProducer(KeyValue properties) {
        this.properties = properties;
        storePath = properties.getString("STORE_PATH");
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
        if (message == null) throw new ClientOMSException("Message should not be null");
        String topic = message.headers().getString(MessageHeader.TOPIC);
        String queue = message.headers().getString(MessageHeader.QUEUE);
        if ((topic == null && queue == null) || (topic != null && queue != null)) {
            throw new ClientOMSException(String.format("Queue:%s Topic:%s should put one and only one", true, queue));
        }

        String bucket = topic != null ? topic : queue;
        PrintWriter pw = printWriterHashMap.putIfAbsent(bucket, messageStore.putBucketFile(storePath, bucket));
        pw.println(message.toString());
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
        for (PrintWriter pw : printWriterHashMap.values()) {
            pw.flush();
        }
    }
}
