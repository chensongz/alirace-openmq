package io.openmessaging.demo;

import io.openmessaging.Message;
import io.openmessaging.MessageHeader;

import java.io.ByteArrayOutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

public class MappedReader {

    private final int INI = 0;
    private final int BODY = 1;
    private final int HEAD = 2;
    private final int PROP = 3;
    private final int END = 4;

    private FileChannel fc;
    private MappedByteBuffer buf;
    private String filename;

    private ByteArrayOutputStream bao;
    private StringBuilder stringBuilder = new StringBuilder();
    private int state;

    public MappedReader(String storePath, String filename) {
        this.filename = filename;
        try {
            fc = new RandomAccessFile(storePath + "/" + filename, "r").getChannel();
            map(0);
            bao = new ByteArrayOutputStream();
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
                setProp(message);
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
        bao.reset();
        while ((curr = buf.get()) != MessageFlag.FIELD_END) {
            bao.write(curr);
        }
        state = HEAD;
        return new DefaultBytesMessage(bao.toByteArray());
    }

    private void setHead(Message message) {
        if (filename.startsWith("T")) {
            message.putHeaders(MessageHeader.TOPIC, filename);
        } else {
            message.putHeaders(MessageHeader.QUEUE, filename);
        }
        message.putHeaders(MessageHeader.MESSAGE_ID, readString(MessageFlag.FIELD_END));
        state = PROP;
    }

    private void setHead2(Message message) {
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
                    if (buf.get() == MessageFlag.QUEUE_PREFIX) {
                        message.putHeaders(MessageHeader.QUEUE, MessageFlag.QUEUE_STR_PREFIX + readString(MessageFlag.VALUE_END));
                    } else {
                        System.out.println("not in QUEUE_PREFIX");
                        buf.position(buf.position() - 1);
                        setStringHead(MessageHeader.QUEUE, message);
                    }
                    break;
                case MessageFlag.TOPIC:
                    if (buf.get() == MessageFlag.TOPIC_PREFIX) {
                        message.putHeaders(MessageHeader.TOPIC, MessageFlag.TOPIC_STR_PREFIX + readString(MessageFlag.VALUE_END));
                    } else {
                        System.out.println("not in TOPIC_PREFIX");
                        buf.position(buf.position() - 1);
                        setStringHead(MessageHeader.TOPIC, message);
                    }
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

    private void setProp(Message message) {
        stringBuilder.setLength(0);
        stringBuilder.append(MessageFlag.PRODUCER_STR_PREFIX)
                .append(buf.get() & 0xff)
                .append('_');
        byte[] offset = new byte[3];
        buf.get(offset, 0, 3);
        stringBuilder.append(bytes2String(offset));
        message.putProperties(MessageFlag.PRO_OFFSET_KEY, stringBuilder.toString());
        byte curr;
        while ((curr = buf.get()) != MessageFlag.MESSAGE_START) {
            if (curr == 0) {
                break;
            }
            buf.position(buf.position() - 1);
            String key = readString(MessageFlag.KEY_END);
            String value = readString(MessageFlag.VALUE_END, MessageFlag.MESSAGE_START);
            message.putProperties(key, value);
        }
        buf.position(buf.position() - 1);
        state = END;
    }

    private String bytes2String(byte[] b) {
        int num = 0;
        num += b[2] & 0xff;
        num += (b[1] & 0xff) << 8;
        num += (b[0] & 0xff) << 16;
        return String.valueOf(num);
    }

    private void setProp2(Message message) {
        byte curr;
        while ((curr = buf.get()) != MessageFlag.MESSAGE_START) {
            if (curr == 0) {
                break;
            }
            switch (curr) {
                case MessageFlag.PRO_OFFSET:
                    if (buf.get() == MessageFlag.PRODUCER_PREFIX) {
                        message.putProperties("PRO_OFFSET", MessageFlag.PRODUCER_STR_PREFIX + readString(MessageFlag.VALUE_END));
                    } else {
                        System.out.println("not in PRODUCER_PREFIX");
                        buf.position(buf.position() - 1);
                        setStringProp("PRO_OFFSET", message);
                    }
                    break;
                default:
                    buf.position(buf.position() - 1);
                    String key = readString(MessageFlag.KEY_END);
                    setStringProp(key, message);
                    break;
            }
        }
        buf.position(buf.position() - 1);
        state = END;
    }

    private String readString(byte end) {
        byte t;
        bao.reset();
        while ((t = buf.get()) != end) {
            bao.write(t);
        }
        return bao.toString();
    }

    private String readString(byte end1, byte end2) {
        byte t = buf.get();
        bao.reset();
        while (t != end1 && t != end2 && t != 0) {
            bao.write(t);
            t = buf.get();
        }
        if (t == MessageFlag.MESSAGE_START) {
            buf.position(buf.position() - 1);
        }
        return bao.toString();
    }

    private void setStringHead(String key, Message message) {
        message.putHeaders(key, readString(MessageFlag.VALUE_END));
    }

    private void setStringProp(String key, Message message) {
        message.putProperties(key, readString(MessageFlag.VALUE_END));
    }
}
