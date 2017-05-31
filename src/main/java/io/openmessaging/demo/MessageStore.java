package io.openmessaging.demo;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, MappedWriter> bufferBuckets = new ConcurrentHashMap<>(1024);

    public synchronized MappedWriter getMappedWriter(String storePath, String bucket) {
        String fileName = storePath + "/" + bucket;
        MappedWriter mw, ret;
        if (!bufferBuckets.containsKey(bucket)) {
            mw = new MappedWriter(fileName);
            ret = bufferBuckets.putIfAbsent(bucket, mw);

            if (ret == null) return mw;
            else {
                mw.close();
                return ret;
            }
        } else {
            return bufferBuckets.get(bucket);
        }
    }
}
