package io.openmessaging.demo;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
        PrintWriter ret, pw;

        ret = null;
        try {
            pw = new PrintWriter(new BufferedWriter(new FileWriter(fileName), 819200));
            ret = printWriterBuckets.putIfAbsent(bucket, pw);

            if(ret == null) ret = pw;
            else pw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }
}
