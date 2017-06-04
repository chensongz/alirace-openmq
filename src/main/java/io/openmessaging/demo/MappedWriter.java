package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.KeyValue;
import io.openmessaging.MessageHeader;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class MappedWriter {

    private static final long SIZE = 16 * 1024 * 1024;
    private static final long MAX_MESSAGE_SIZE = 256 * 1024;

    private FileChannel fc;
    private MappedByteBuffer buf;
    private long offset; //write offset in the whole file

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
        if (MAX_MESSAGE_SIZE > buf.remaining()) {
            offset += buf.position();
            map(offset);
        }

        buf.put(MessageFlag.MESSAGE_START);
        putBody(message.getBody());
        buf.put(MessageFlag.FIELD_END);
        putHeaders(message.headers());
        buf.put(MessageFlag.FIELD_END);
        putProperties(message.properties());
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
        buf.put(headers.getString(MessageHeader.MESSAGE_ID).getBytes());
    }

    private void putProperties(KeyValue properties) {
        String producerValue = properties.getString(MessageFlag.PRO_OFFSET_KEY);
        String[] str = producerValue.substring(8).split("_", 2);
        buf.put(Byte.parseByte(str[0]));
        buf.put(stringNum2Bytes(str[1]));
        int size = properties.keySet().size();
        int i = 0;
        for (String key : properties.keySet()) {
            i++;
            if (!key.equals(MessageFlag.PRO_OFFSET_KEY)) {
                buf.put(key.getBytes());
                buf.put(MessageFlag.KEY_END);
                buf.put(properties.getString(key).getBytes());
                if (i < size) {
                    buf.put(MessageFlag.VALUE_END);
                }
            }
        }
    }

    private byte[] stringNum2Bytes(String s) {
        int num = Integer.parseInt(s);
        byte[] ret = new byte[3];
        ret[0] = (byte) ((num & 0xff0000) >>> 16);
        ret[1] = (byte) ((num & 0xff00) >>> 8);
        ret[2] = (byte) ((num & 0xff));
        return ret;
    }


    private void putHeaders2(KeyValue headers) {
        for (String key : headers.keySet()) {
            String value = headers.getString(key);
            switch (key) {
                case MessageHeader.MESSAGE_ID:
                    buf.put(MessageFlag.MESSAGE_ID);
                    buf.put(value.getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.TOPIC:
                    buf.put(MessageFlag.TOPIC);
                    if (value.startsWith(MessageFlag.TOPIC_STR_PREFIX)) {
                        buf.put(MessageFlag.TOPIC_PREFIX);
                        buf.put(value.substring(6).getBytes());
                    } else {
                        buf.put(value.getBytes());
                    }
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.QUEUE:
                    buf.put(MessageFlag.QUEUE);
                    if (value.startsWith(MessageFlag.QUEUE_STR_PREFIX)) {
                        buf.put(MessageFlag.QUEUE_PREFIX);
                        buf.put(value.substring(6).getBytes());
                    } else {
                        buf.put(value.getBytes());
                    }
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
                    buf.put(value.getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.PRIORITY:
                    buf.put(MessageFlag.PRIORITY);
                    buf.put(value.getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.RELIABILITY:
                    buf.put(MessageFlag.RELIABILITY);
                    buf.put(value.getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.SCHEDULE_EXPRESSION:
                    buf.put(MessageFlag.SCHEDULE_EXPRESSION);
                    buf.put(value.getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.SEARCH_KEY:
                    buf.put(MessageFlag.SEARCH_KEY);
                    buf.put(value.getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.SHARDING_KEY:
                    buf.put(MessageFlag.SHARDING_KEY);
                    buf.put(value.getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.STORE_HOST:
                    buf.put(MessageFlag.STORE_HOST);
                    buf.put(value.getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                case MessageHeader.TRACE_ID:
                    buf.put(MessageFlag.TRACE_ID);
                    buf.put(value.getBytes());
                    buf.put(MessageFlag.VALUE_END);
                    break;
                default:
                    System.out.println("Put message headers error!!Unkown key " + key);
                    break;
            }
        }
    }

    private void putProperties2(KeyValue properties) {
        for (String key : properties.keySet()) {
            switch (key) {
                case "PRO_OFFSET":
                    buf.put(MessageFlag.PRO_OFFSET);
                    String value = properties.getString(key);
                    if (value.startsWith(MessageFlag.PRODUCER_STR_PREFIX)) {
                        buf.put(MessageFlag.PRODUCER_PREFIX);
                        buf.put(value.substring(8).getBytes());
                    } else {
                        buf.put(value.getBytes());
                    }

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
