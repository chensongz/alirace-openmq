package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MappedReader {

    private final int INI = 0;
    private final int BODY = 1;
    private final int HEAD = 2;
    private final int PROP = 3;
    private final int END = 4;

    private FileChannel fc;
    private MappedByteBuffer buf;

    private int state;

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
        state = INI;
        byte curr;
        Message message = null;
        while (state != END) {
            if (state == INI) {
                curr = buf.get();
                if (curr == MessageFlag.MESSAGE_START) {
                    state = BODY;
                } else {
                    return null;
                }
            } else if (state == BODY) {
                message = setBody();
            } else if (state == HEAD) {
                setHead(message);
            } else if (state == PROP) {
                setPROP(message);
            } else if (state == END) {
                break;
            }
        }
        return message;
    }

    public void close() {
        try {
            fc.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private Message setBody() {
        byte curr;
        List<Byte> bodyArray = new ArrayList<>();
        while ((curr = buf.get()) != MessageFlag.FIELD_END) {
            bodyArray.add(curr);
        }
        state = HEAD;
        byte[] body = new byte[bodyArray.size()];
        for (int i = 0; i < bodyArray.size(); i++) {
            body[i] = bodyArray.get(i);
        }
        return new DefaultBytesMessage(body);
    }

    private void setHead(Message message) {
        byte curr;
        while ((curr = buf.get()) != MessageFlag.FIELD_END) {
            switch (curr) {
                case MessageFlag.BORN_TIMESTAMP:
                    message.putHeaders(MessageHeader.BORN_TIMESTAMP, buf.getLong());
                    buf.get();
                    break;
                case MessageFlag.START_TIME:
                    message.putHeaders(MessageHeader.START_TIME, buf.getLong());
                    buf.get();
                    break;
                case MessageFlag.STOP_TIME:
                    message.putHeaders(MessageHeader.START_TIME, buf.getLong());
                    buf.get();
                    break;
                case MessageFlag.STORE_TIMESTAMP:
                    message.putHeaders(MessageHeader.STORE_TIMESTAMP, buf.getLong());
                    buf.get();
                    break;
                case MessageFlag.TIMEOUT:
                    message.putHeaders(MessageHeader.TIMEOUT, buf.getLong());
                    buf.get();
                    break;
                case MessageFlag.QUEUE:
                    setStringHead(MessageHeader.QUEUE, message);
                    break;
                case MessageFlag.TOPIC:
                    setStringHead(MessageHeader.TOPIC, message);
                    break;
                case MessageFlag.MESSAGE_ID:
                    setStringHead(MessageHeader.MESSAGE_ID, message);
                    break;
                case MessageFlag.BORN_HOST:
                    setStringHead(MessageHeader.BORN_HOST, message);
                    break;
                case MessageFlag.PRIORITY:
                    setStringHead(MessageHeader.PRIORITY, message);
                    break;
                case MessageFlag.RELIABILITY:
                    setStringHead(MessageHeader.RELIABILITY, message);
                    break;
                case MessageFlag.SCHEDULE_EXPRESSION:
                    setStringHead(MessageHeader.SCHEDULE_EXPRESSION, message);
                    break;
                case MessageFlag.SEARCH_KEY:
                    setStringHead(MessageHeader.SEARCH_KEY, message);
                    break;
                case MessageFlag.SHARDING_KEY:
                    setStringHead(MessageHeader.SHARDING_KEY, message);
                    break;
                case MessageFlag.SHARDING_PARTITION:
                    setStringHead(MessageHeader.SHARDING_PARTITION, message);
                    break;
                case MessageFlag.STORE_HOST:
                    setStringHead(MessageHeader.STORE_HOST, message);
                    break;
                case MessageFlag.TRACE_ID:
                    setStringHead(MessageHeader.TRACE_ID, message);
                    break;
                default:
                    break;
            }
        }
        state = PROP;
    }

    private void setPROP(Message message) {
        byte curr;
        while ((curr = buf.get()) != MessageFlag.MESSAGE_END) {
            switch (curr) {
                case MessageFlag.PRO_OFFSET:
                    setStringProp("PRO_OFFSET", message);
                    break;
                default:
                    buf.position(buf.position() - 1);
                    String key = readString(MessageFlag.KEY_END);
                    setStringProp(key, message);
                    break;
            }
        }
        state = END;
    }

    private String readString(byte end) {
        byte t;
        List<Byte> v = new ArrayList<>();
        while ((t = buf.get()) != end) {
            v.add(t);
        }
        byte[] value = new byte[v.size()];
        for (int i = 0; i < v.size(); i++) {
            value[i] = v.get(i);
        }
        return new String(value);
    }

    private void setStringHead(String key, Message message) {
        message.putHeaders(key, readString(MessageFlag.VALUE_END));
    }

    private void setStringProp(String key, Message message) {
        message.putProperties(key, readString(MessageFlag.VALUE_END));
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
