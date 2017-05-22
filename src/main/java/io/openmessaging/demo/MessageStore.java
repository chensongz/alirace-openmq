package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, PrintWriter> printWriterBuckets = new HashMap<>();

    public synchronized void putMessage(String storePath, String bucket, Message message) {
        if (!printWriterBuckets.containsKey(bucket)) {
            String fileName = storePath + "/" + bucket;
            try {
                printWriterBuckets.put(bucket, new PrintWriter(new FileWriter(fileName), true));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        PrintWriter printWriter = printWriterBuckets.get(bucket);
        printWriter.println(message.toString());
    }
}
