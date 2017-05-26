package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Created by bgk on 5/22/17.
 */
public class MessageDrawer {
    private LinkedList<Message> messageQueue = new LinkedList<>();
    private LinkedList<String> nonConsumeFiles = new LinkedList<>();
    private String storePath;
    private BufferedReader currentReader;
    // can modify to satisfy memory need.
    // if memory is big, readCount can be bigger, vice versa.
    private int readCount = 100;

    public MessageDrawer(String storePath) {
        this.storePath = storePath;
    }

    private void readFile(String fileName) {
        File file = new File(fileName);
        System.out.println("file name: " + file.getAbsolutePath() + " file size: " + file.length());
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file), 8192);
            String row;
            int num = 0;
            while ((row = reader.readLine()) != null) {
                messageQueue.add(parseMessage(row));
                num++;
                if (num >= readCount) {
                    break;
                }
            }
            currentReader = reader;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void attachQueue(String queueName, Collection<String> topics) {
        readFile(storePath + "/" + queueName);
        nonConsumeFiles.addAll(topics);
    }

    public Message pullMessage() {
        Message message = messageQueue.poll();
        if (message == null) {
            try {
                String row;
                int num = 0;
                while ((row = currentReader.readLine()) != null) {
                    messageQueue.add(parseMessage(row));
                    num++;
                    if (num >= readCount) {
                        break;
                    }
                }
                if (num < readCount) {
                    currentReader.close();
                    String nonConsumeFileName = nonConsumeFiles.poll();
                    if (nonConsumeFileName != null) {
                        readFile(storePath + "/" + nonConsumeFileName);
                        message = messageQueue.poll();
                    }
                } else {
                    message = messageQueue.poll();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return message;
    }

    public Message parseMessage(String row) {
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
