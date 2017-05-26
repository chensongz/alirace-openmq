package io.openmessaging.demo;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, PrintWriter> printWriterBuckets = new ConcurrentHashMap<>(1024);

    public PrintWriter putBucketFile(String storePath, String bucket) {
        String fileName = storePath + "/" + bucket;
        try {
            printWriterBuckets.putIfAbsent(bucket, new PrintWriter(new BufferedWriter(new FileWriter(fileName), 65536)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return printWriterBuckets.get(bucket);
    }
}
