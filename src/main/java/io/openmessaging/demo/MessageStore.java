package io.openmessaging.demo;

import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class MessageStore {

    private static final MessageStore INSTANCE = new MessageStore();

    public static MessageStore getInstance() {
        return INSTANCE;
    }

    private Map<String, MappedWriter> bufferBuckets = new ConcurrentHashMap<>(1024);


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

    public synchronized MappedWriter getMappedWriter(String storePath, String bucket) {
        String fileName = storePath + "/" + bucket;
        MappedWriter mw, ret;
        if (!bufferBuckets.containsKey(bucket)) {
            mw = new MappedWriter(fileName);
            ret = bufferBuckets.putIfAbsent(bucket, mw);

            if(ret == null) return mw;
            else{
                mw.close();
                return ret;
            }
        } else {
            return bufferBuckets.get(bucket);
        }
    }

//    public synchronized void putBucketFile(String storePath, String bucket, int msgSize, byte[] msg) {
//        String fileName = storePath + "/" + bucket;
//        FileChannel fc = null;
//        MappedByteBuffer buf = null;
//        int offset = 0;
//        try{
//            if(!channelBuckets.containsKey(bucket)){
//                fc = new RandomAccessFile(fileName, "rw").getChannel();
//                buf = fc.map(FileChannel.MapMode.READ_WRITE, 0, SIZE);
//                channelBuckets.put(bucket, fc);
//                offsetBuckets.put(bucket, 0);
//                bufferBuckets.put(bucket, buf);
//            }else{
//                fc = channelBuckets.get(bucket);
//                offset = offsetBuckets.get(bucket);
//                buf = bufferBuckets.get(bucket);
//            }
//
//            //whether to remap
//            if(buf.remaining() < msgSize){
//                offset += buf.position();
//                buf.force();
//                buf = fc.map(FileChannel.MapMode.READ_WRITE, offset, SIZE);
//                offsetBuckets.put(bucket, offset);
//                bufferBuckets.put(bucket, buf);
//            }
//            buf.put(msg);
//        }catch(Exception e){
//            e.printStackTrace();
//        }
//
//
//    }
}
