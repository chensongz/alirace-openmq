package io.openmessaging.demo;


import io.openmessaging.Message;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedReader {

    private FileChannel fc;
    private MappedByteBuffer buf;

    public MappedReader(String filename) {
        try {
            fc = new RandomAccessFile(filename, "r").getChannel();
            map(0);
            //for test
//            System.out.printf("file %s size: %d", filename, fc.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void map(long offset) {
        try {
            buf = fc.map(FileChannel.MapMode.READ_ONLY, offset, fc.size());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Message poll() {
        int msgLen = buf.getInt();

        byte[] msg = new byte[msgLen];
        buf.get(msg, 0, msgLen);
        //for test
//        System.out.println("### msg: " + new String(msg));
        char tail = buf.getChar();
        if (tail != '$') return null;
        else return parseMessage(new String(msg));
    }

    public void close() {
        try {
            fc.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Message parseMessage(String row) {
        String[] splitRow = row.split("\\|");
        String propertiesString = splitRow[0];
        String headersString = splitRow[1];
        String body = splitRow[2];
        Message message = new DefaultBytesMessage(body.getBytes());
        if (!propertiesString.equals("")) {
            String[] properties = propertiesString.split("\t");
            for (String kvStr : properties) {
                String[] kv = kvStr.split(":");
                message.putProperties(kv[0], kv[1]);
            }
        }
        if (!headersString.equals("")) {
            String[] headers = headersString.split("\t");
            for (String kvStr : headers) {
                String[] kv = kvStr.split(":");
                message.putHeaders(kv[0], kv[1]);
            }
        }

        return message;
    }


}
