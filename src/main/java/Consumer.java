import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class Consumer implements Runnable {
    Connection connection;
    ConcurrentHashMap<String, SkiEvent> storage;

    public Consumer(Connection connection, ConcurrentHashMap<String, SkiEvent> db) {
        this.connection = connection;
        this.storage = db;
    }

    @Override
    public void run() {
        try {
            Channel channel = connection.createChannel();
            boolean durable = false;
            channel.queueDeclare(MultiThreadedConsumer.QUEUE_NAME, durable, false, false, null);

            System.out.printf(" [*] Waiting for messages. To Exit press CTRL+C\n");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                Gson gson = new Gson();
                SkiEvent skiEvent = gson.fromJson(message, SkiEvent.class);
                String id = UUID.randomUUID().toString();
                long messageId = delivery.getEnvelope().getDeliveryTag();
                storage.put(id, skiEvent);
                channel.basicAck(messageId, true);
//                System.out.printf("[x] Received Message: routed to %s - %s \n", delivery.getEnvelope().getRoutingKey(), message);
                System.out.printf("Messages in hashmap %d\n", storage.size());
            };

            boolean autoAck = false;
            channel.basicConsume(MultiThreadedConsumer.QUEUE_NAME, autoAck, deliverCallback, consumerTag -> {
            });

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
