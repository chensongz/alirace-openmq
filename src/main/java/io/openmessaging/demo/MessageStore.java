package io.openmessaging.demo;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, PrintWriter> printWriterBuckets = new HashMap<>();

    public synchronized PrintWriter putBucketFile(String storePath, String bucket) {
        if (!printWriterBuckets.containsKey(bucket)) {
            String fileName = storePath + "/" + bucket;
            try {
                printWriterBuckets.put(bucket, new PrintWriter(new BufferedWriter(new FileWriter(fileName), 8192000)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return printWriterBuckets.get(bucket);
    }
}
