package io.openmessaging.demo;


import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.Message;

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

    public void send(BytesMessage message) {
        buf.put(MessageFlag.MESSAGE_START);
        putHeaders(message.headers());
        putProperties(message.properties());
        putBody(message.getBody());
        buf.put(MessageFlag.MESSAGE_END);

        byte[] msg = message.toString().getBytes();
        int msgLen = msg.length;
        int totLen = 4 + 2 + msgLen;
        //for test
//        System.out.println("### msgLen: " + msgLen);

        synchronized (lock) {
            if (totLen > buf.remaining()) {
                offset += buf.position();
                map(offset);
                //for test
//                System.out.printf("############## remaped offset %s ################", offset);
            }
            buf.putInt(msgLen);
            buf.put(msg);
            buf.putChar('$');
            //for test
//            System.out.println("### fc info: " + fc.toString());
//            System.out.println("### buf info: " + buf.toString());
        }
    }

    public void close() {
        try {
            fc.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void putHeaders(KeyValue headers) {

    }

    private void putProperties(KeyValue properties) {

    }

    private void putBody(byte[] body) {

    }

}
