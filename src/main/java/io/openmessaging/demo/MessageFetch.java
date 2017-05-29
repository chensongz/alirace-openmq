package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

public class MessageFetch {
    private LinkedList<Message> messageQueue = new LinkedList<>();
    private LinkedList<String> nonConsumeFiles = new LinkedList<>();
    private String storePath;
    private MappedReader currentReader;
    // can modify to satisfy memory need.
    // if memory is big, readCount can be bigger, vice versa.
    private int readCount = 1000;

    public MessageFetch(String storePath) {
        this.storePath = storePath;
    }

    private void readFile(String fileName) {
        try {
            MappedReader reader = new MappedReader(fileName);
            Message message;
            int num = 0;

            while ((message = reader.poll()) != null) {
                messageQueue.add(message);
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
                Message row;
                int num = 0;
                while ((row = currentReader.poll()) != null) {
                    messageQueue.add(row);
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
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return message;
    }

}
