package io.openmessaging.demo;

import io.openmessaging.BytesMessage;
import io.openmessaging.Message;

import java.io.File;
import java.util.Collection;
import java.util.LinkedList;

public class MessageFetch {
    private LinkedList<Message> messageQueue = new LinkedList<>();
    private LinkedList<String> nonConsumeFiles = new LinkedList<>();
    private String storePath;
    private MappedReader currentReader;
    // can modify to satisfy memory need.
    // if memory is big, readCount can be bigger, vice versa.
    private int readCount = 50;

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
//            System.out.println("messageQueue :" + messageQueue);
//            System.out.println("reader:" + reader.toString());
            currentReader = reader;
        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    private void filteredFilenames(String[] filenames, Collection<String> prefixes) {
        for(String filename: filenames){
            for(String prefix: prefixes){
                if(filename.startsWith(prefix)){
                    nonConsumeFiles.add(filename);
                    break;
                }
            }
        }
    }

    public void attachQueue(String queueName, Collection<String> topics) {
//        File dir = new File(storePath);
//        String[] files = dir.list();
//        LinkedList<String> topicsAndQueue = new LinkedList<>();
//        topicsAndQueue.add(queueName);
//        topicsAndQueue.addAll(topics);
//        filteredFilenames(files, topicsAndQueue);
//
//        String starter = nonConsumeFiles.poll();
//        readFile(storePath + "/" + starter);
//        System.out.println("starter:" + storePath + "/" + starter);
        readFile(storePath + "/" + queueName);
        nonConsumeFiles.addAll(topics);
    }

    public Message pullMessage() {
        Message message = messageQueue.poll();
//        System.out.println("message1:" + message);
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
//                    currentReader.close();
//                    System.out.println("message num:" + num);
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
//        System.out.println("message2:" + message);
//        System.out.println("message3:" + message.headers().keySet() +
//                ":" + ((DefaultKeyValue)message.headers()).values()+ "|"
//                + new String(((BytesMessage)message).getBody()) + "|"  + message.properties().keySet() + ":"
//                + ((DefaultKeyValue)message.properties()).values());

        return message;
    }

}
