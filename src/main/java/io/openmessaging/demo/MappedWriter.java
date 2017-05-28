package io.openmessaging.demo;


import io.openmessaging.Message;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedWriter {

    private final long SIZE = 32 * 1024 * 1024;

    private FileChannel fc;
    private MappedByteBuffer buf;
    private long offset; //上次map得到的buffer的开头位置

    public MappedWriter(String filename){
        try{
            fc = new RandomAccessFile(filename, "rw").getChannel();
            offset = 0;
            map(offset);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    private void map(long offset){
        try{
            buf = fc.map(FileChannel.MapMode.READ_WRITE, offset, SIZE);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public synchronized void send(Message message){
        byte[] msg = message.toString().getBytes();
        int msgLen = msg.length;
        int totLen = 4 + 1 + msgLen;

        if(totLen > buf.remaining()){
            offset += buf.position();
            map(offset);
        }

        buf.putInt(msgLen);
        buf.put(msg);
        buf.putChar('$');
    }

}
