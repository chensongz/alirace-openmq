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

    private Map<String, FileWriter> fileWriterBuckets = new ConcurrentHashMap<>(1024);

    public FileWriter putBucketFile(String storePath, String bucket) {
        String fileName = storePath + "/" + bucket;
        FileWriter ret, fw;
        
        ret = null;
        try {
            fw = new FileWriter(fileName);
            ret = fileWriterBuckets.putIfAbsent(bucket, fw);

            fw = new FileWriter(fileName);

            if(ret == null) ret = fw;
            else fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }
}
