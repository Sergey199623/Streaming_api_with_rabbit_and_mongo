package Controllers;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

public class MessageThread extends Thread {

    private Channel channel;
    private String message;
    private long tag;

    public MessageThread(Channel channel, String message, long tag) {
        this.channel = channel;
        this.message = message;
        this.tag = tag;
    }

    @Override
    public void run() {
        try {
            System.err.println("Message received " + message);
            sleep(5000); // имитируем обработку сообщения
            channel.basicAck(tag, false);
            System.err.println("Message deleted " + message);
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
        }
    }
}