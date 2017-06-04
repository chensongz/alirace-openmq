package io.openmessaging.demo;

import io.openmessaging.Message;

import java.util.Collection;
import java.util.LinkedList;

public class MessageFetch {
    private LinkedList<String> nonConsumeFiles = new LinkedList<>();
    private String storePath;
    private MappedReader currentReader;

    public MessageFetch(String storePath) {
        this.storePath = storePath;
    }

    public void attachQueue(String queueName, Collection<String> topics) {
        currentReader = new MappedReader(storePath + "/" + queueName);
        nonConsumeFiles.addAll(topics);
    }

    public Message pullMessage() {
        Message message;
        message = currentReader.poll();
        if (message == null) {
            String nonConsumeFileName = nonConsumeFiles.poll();
            if (nonConsumeFileName != null) {
                currentReader = new MappedReader(storePath + "/" + nonConsumeFileName);
                return currentReader.poll();
            }
        }
        return message;
    }

}
