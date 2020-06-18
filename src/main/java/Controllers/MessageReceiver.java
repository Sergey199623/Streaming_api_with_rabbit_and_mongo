package Controllers;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class MessageReceiver {

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        Connection conn = factory.newConnection();
        Channel channel = conn.createChannel();
        String exchangeName = "myExchange";
        String queueName = "myQueue";
        String routingKey = "testRoute";
        boolean durable = true;
        channel.exchangeDeclare(exchangeName, "direct", durable);
        channel.queueDeclare(queueName, durable, false, false, null);
        channel.queueBind(queueName, exchangeName, routingKey);
        boolean run = true;
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            new MessageThread(channel, new String(delivery.getBody()), delivery.getEnvelope().getDeliveryTag()).start();
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
        channel.close();
        conn.close();
    }
}