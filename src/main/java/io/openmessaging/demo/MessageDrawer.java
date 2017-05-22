package io.openmessaging.demo;

import io.openmessaging.Message;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.LinkedList;


public class MessageDrawer {

    private static final MessageDrawer INSTANCE = new MessageDrawer();

    public static MessageDrawer getInstance() {
        return INSTANCE;
    }

    private LinkedList<Message> messages = new LinkedList<>();
    private int i = 0;

    public synchronized LinkedList<Message> loadFromDisk(String storePath) {
        if (messages.isEmpty()) {
            System.out.println(i + "times to load from disk");
            try {
                File dir = new File(storePath);
                if (!dir.exists()) {
                    System.out.println(storePath + " not exists");
                } else {
                    File[] files = dir.listFiles();
//                    if (files != null) {
//                        for (File file : files) {
//                            System.out.println("file path:" + file.getAbsolutePath() + " " + file.length());
//                            BufferedReader reader = new BufferedReader(new FileReader(file));
//                            String row;
//                            while ((row = reader.readLine()) != null) {
//                                Message message = parseMessage(row);
//                                messages.add(message);
//                            }
//                            reader.close();
////                            messages.add(parseMessage("|Queue:QUEUE2\t|body:QUEUE21023"));
//                        }
//
//                    }
                    if (files != null) {
                        File file = files[0];
                        System.out.println("file path:" + file.getAbsolutePath() + " " + file.length());
                        BufferedReader reader = new BufferedReader(new FileReader(file));
                        String row;
                        while ((row = reader.readLine()) != null) {
                            messages.add(parseMessage(row));
                        }
                        reader.close();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return messages;
    }

    public Message parseMessage(String row) {
        String[] splitRow = row.split("\\|");
        String propertiesString = splitRow[0];
        String headersString = splitRow[1];
        String bodyString = splitRow[2];
        String body = bodyString.split(":")[1];
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
