package io.openmessaging.demo;

import io.openmessaging.KeyValue;
import io.openmessaging.Message;
import io.openmessaging.PullConsumer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DefaultPullConsumer implements PullConsumer {
    private MessageDrawer messageDrawer;
    private KeyValue properties;
    private String queue;

    public DefaultPullConsumer(KeyValue properties) {
        this.properties = properties;
        String storePath = properties.getString("STORE_PATH");
        messageDrawer = new MessageDrawer(storePath);
    }


    @Override public KeyValue properties() {
        return properties;
    }


    @Override public Message poll() {
        return messageDrawer.pullMessage();
    }

    @Override public Message poll(KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void ack(String messageId, KeyValue properties) {
        throw new UnsupportedOperationException("Unsupported");
    }

    @Override public void attachQueue(String queueName, Collection<String> topics) {
        if (queue != null && !queue.equals(queueName)) {
            throw new ClientOMSException("You have alreadly attached to a queue " + queue);
        }
        queue = queueName;
        String str = "";
        for (String topic:topics) {
            str += topic + ",";
        }
        System.out.println(this.toString() + " attachQueue: " + queueName + " topics: [" + str + "]");
        messageDrawer.attachQueue(queueName, topics);
    }


}
