package io.openmessaging.demo;

import java.io.*;
import java.util.Map;
import java.util.HashMap;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }
    private Map<String, PrintWriter> fileWriterBuckets = new HashMap<>(1024);

    public synchronized PrintWriter putBucketFile(String storePath, String bucket) {

        String fileName = storePath + "/" + bucket;

        PrintWriter pw = null;
        try {
            if(!fileWriterBuckets.containsKey(bucket)){
                pw = new PrintWriter(new BufferedWriter(new FileWriter(fileName), 819200));
            }else{
                pw = fileWriterBuckets.get(bucket);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return pw;
    }
}
