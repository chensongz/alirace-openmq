package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.MessageHeader;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedWriter {

    private static final long SIZE = 32 * 1024 * 1024;

    private FileChannel fc;
    private MappedByteBuffer buf;
    private long offset; //write offset in the whole file

    private final Object lock = new Object();

    public MappedWriter(String filename) {
        try {
            fc = new RandomAccessFile(filename, "rw").getChannel();
            offset = 0;
            map(offset);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void map(long offset) {
        try {
            buf = fc.map(FileChannel.MapMode.READ_WRITE, offset, SIZE);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized void send(BytesMessage message) {
        //todo remap
        if (256 > buf.remaining()) {
            offset += buf.position();
            map(offset);
        }

        buf.put(MessageFlag.MESSAGE_START);
        putBody(message.getBody());
        buf.put(MessageFlag.FIELD_END);
        putHeaders(message.headers());
        buf.put(MessageFlag.FIELD_END);
        putProperties(message.properties());
        buf.put(MessageFlag.MESSAGE_END);

//        byte[] msg = message.toString().getBytes();
//        int msgLen = msg.length;
//        int totLen = 4 + 2 + msgLen;
//        //for test
////        System.out.println("### msgLen: " + msgLen);
//
//        synchronized (lock) {
//            if (totLen > buf.remaining()) {
//                offset += buf.position();
//                map(offset);
//                //for test
////                System.out.printf("############## remaped offset %s ################", offset);
//            }
//            buf.putInt(msgLen);
//            buf.put(msg);
//            buf.putChar('$');
//            //for test
////            System.out.println("### fc info: " + fc.toString());
////            System.out.println("### buf info: " + buf.toString());
//    }

    }

    public void close() {
        try {
            fc.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void putBody(byte[] body) {
        buf.put(body);
    }

    private void putHeaders(KeyValue headers) {
        for (String key : headers.keySet()) {
            switch (key) {
                case MessageHeader.MESSAGE_ID:
                    buf.put(MessageFlag.MESSAGE_ID);
                    buf.put(headers.getString(key).getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.TOPIC:
                    buf.put(MessageFlag.TOPIC);
                    buf.put(headers.getString(key).getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.QUEUE:
                    buf.put(MessageFlag.QUEUE);
                    buf.put(headers.getString(key).getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.BORN_TIMESTAMP:
                    buf.put(MessageFlag.BORN_TIMESTAMP);
                    buf.putLong(headers.getLong(key));
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.STORE_TIMESTAMP:
                    buf.put(MessageFlag.STORE_TIMESTAMP);
                    buf.putLong(headers.getLong(key));
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.START_TIME:
                    buf.put(MessageFlag.START_TIME);
                    buf.putLong(headers.getLong(key));
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.STOP_TIME:
                    buf.put(MessageFlag.STOP_TIME);
                    buf.putLong(headers.getLong(key));
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.TIMEOUT:
                    buf.put(MessageFlag.TIMEOUT);
                    buf.putLong(headers.getLong(key));
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.BORN_HOST:
                    buf.put(MessageFlag.BORN_HOST);
                    buf.put(headers.getString(key).getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.PRIORITY:
                    buf.put(MessageFlag.PRIORITY);
                    buf.put(headers.getString(key).getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.RELIABILITY:
                    buf.put(MessageFlag.RELIABILITY);
                    buf.put(headers.getString(key).getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.SCHEDULE_EXPRESSION:
                    buf.put(MessageFlag.SCHEDULE_EXPRESSION);
                    buf.put(headers.getString(key).getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.SEARCH_KEY:
                    buf.put(MessageFlag.SEARCH_KEY);
                    buf.put(headers.getString(key).getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.SHARDING_KEY:
                    buf.put(MessageFlag.SHARDING_KEY);
                    buf.put(headers.getString(key).getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.STORE_HOST:
                    buf.put(MessageFlag.STORE_HOST);
                    buf.put(headers.getString(key).getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.TRACE_ID:
                    buf.put(MessageFlag.TRACE_ID);
                    buf.put(headers.getString(key).getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                default:
                    System.out.println("Put message headers error!!Unkown key " + key);
                    break;
            }
        }
    }

    private void putProperties(KeyValue properties) {
        for (String key : properties.keySet()) {
            switch (key) {
                case "PRO_OFFSET":
                    buf.put(MessageFlag.PRO_OFFSET);
                    buf.put(MessageFlag.KEY_END);
                    buf.put(properties.getString(key).getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                default:
                    buf.put(key.getBytes());
                    buf.put(MessageFlag.KEY_END);
                    buf.put(properties.getString(key).getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
            }
        }
    }

}
