package io.openmessaging.demo;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.HashMap;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();
    private static final long SIZE = 32 * 1024 * 1024;

    public static MessageStore getInstance() {
        return INSTANCE;
    }
//    private Map<String, PrintWriter> fileWriterBuckets = new HashMap<>(1024);
    private Map<String, MappedByteBuffer> bufferBuckets = new HashMap<>(1024);
    private Map<String, FileChannel> channelBuckets = new HashMap<>(1024);
    private Map<String, Integer> offsetBuckets = new HashMap<>(1024);


//    public synchronized PrintWriter putBucketFile(String storePath, String bucket) {
//
//        String fileName = storePath + "/" + bucket;
//
//        PrintWriter pw = null;
//        try {
//            if(!fileWriterBuckets.containsKey(bucket)){
//                pw = new PrintWriter(new BufferedWriter(new FileWriter(fileName), 819200));
//            }else{
//                pw = fileWriterBuckets.get(bucket);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return pw;
//    }

    public void flush(String bucket){
        MappedByteBuffer buf = bufferBuckets.get(bucket);
        buf.force();
    }

    public synchronized void putBucketFile(String storePath, String bucket, int msgSize, byte[] msg) {
        String fileName = storePath + "/" + bucket;
        FileChannel fc = null;
        MappedByteBuffer buf = null;
        int offset = 0;
        try{
            if(!channelBuckets.containsKey(bucket)){
                fc = new RandomAccessFile(fileName, "rw").getChannel();
                buf = fc.map(FileChannel.MapMode.READ_WRITE, 0, SIZE);
                channelBuckets.put(bucket, fc);
                offsetBuckets.put(bucket, 0);
                bufferBuckets.put(bucket, buf);
            }else{
                fc = channelBuckets.get(bucket);
                offset = offsetBuckets.get(bucket);
                buf = bufferBuckets.get(bucket);
            }

            //whether to remap
            if(buf.remaining() < msgSize){
                offset += buf.position();
                buf.force();
                buf = fc.map(FileChannel.MapMode.READ_WRITE, offset, SIZE);
                offsetBuckets.put(bucket, offset);
                bufferBuckets.put(bucket, buf);
            }
            buf.put(msg);
        }catch(Exception e){
            e.printStackTrace();
        }


    }
}
