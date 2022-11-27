import com.github.javafaker.Faker;
import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class Consumer implements Runnable {
    private final AtomicInteger count;
    Connection connection;
    DBConnectionPool dbConnectionPool;
    Faker fakerGenerator;

    public Consumer(Connection connection, DBConnectionPool db, AtomicInteger failedDBAdds) {
        this.connection = connection;
        this.dbConnectionPool = db;
        this.fakerGenerator = new Faker();
        this.count = failedDBAdds;
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
//                persistData(skiEvent);
                channel.basicAck(messageId, true);
                System.out.printf("[x] Received Message: routed to %s - %s \n", delivery.getEnvelope().getRoutingKey(), message);
            };


            DeliverCallback deliverCallback1 = new DeliverCallback() {
                @Override
                public void handle(String consumerTag, Delivery delivery) throws IOException {
                    String message = new String(delivery.getBody(), "UTF-8");
                    Gson gson = new Gson();
                    SkiEvent skiEvent = gson.fromJson(message, SkiEvent.class);
                    String id = UUID.randomUUID().toString();
                    long messageId = delivery.getEnvelope().getDeliveryTag();
//                    System.out.printf("[x] Received Message: routed to %s - %s \n", delivery.getEnvelope().getRoutingKey(), message);

                    java.sql.Connection dbConnection = dbConnectionPool.borrowObject();
                    Statement stmt = null;
                    try {
                        dbConnection.setAutoCommit(false);
                        stmt = dbConnection.createStatement();

                        // check and create a new skier
                        String sql = String.format("SELECT * FROM skier WHERE skier_id=%d;", skiEvent.getSkierId());
                        ResultSet skierResult = stmt.executeQuery(sql);
                        String skierId = "";
                        if (skierResult.next()) {
                            skierId = skierResult.getString("id");
                        } else {
                            skierId = UUID.randomUUID().toString();
                            sql = String.format("INSERT INTO skier (id, name, age, skier_id) "
                                            + "VALUES ('%s', '%s', %d, %d);", skierId, fakerGenerator.name().name().replace("'", ""),
                                    fakerGenerator.number().numberBetween(21, 80), skiEvent.getSkierId());
                            stmt.executeUpdate(sql);
                        }


                        // check and create a new resort
                        sql = String.format("SELECT * FROM resort WHERE resort_id=%d;", skiEvent.getResortId());
                        String resortId = "";
                        ResultSet resortResult = stmt.executeQuery(sql);

                        if (resortResult.next()) {
                            resortId = resortResult.getString("id");
                        } else {
                            resortId = UUID.randomUUID().toString();
                            sql = String.format("INSERT INTO resort (id, name, address, resort_id)  "
                                            + "VALUES ('%s', '%s', '%s', %d);", resortId, fakerGenerator.name().name().replace("'", ""),
                                    fakerGenerator.address().fullAddress().replace("'", ""), skiEvent.getResortId());

                            stmt.executeUpdate(sql);

                        }
                        String randomId = UUID.randomUUID().toString();
                        sql = String.format("INSERT INTO skievent (id, day_id, lift_id, season_id, skier_id, resort_id, time) "
                                        + "VALUES ('%s', %d, %d, %d, '%s', '%s', %d);",
                                randomId,
                                skiEvent.getDayId(),
                                skiEvent.getLiftId(),
                                skiEvent.getSeasonId(),
                                skierId,
                                resortId,
                                skiEvent.getTime()
                        );
                        stmt.executeUpdate(sql);
                        stmt.close();
                        dbConnection.commit();
                    } catch (SQLException e) {
                        System.out.println(e.getMessage());
//                        System.out.println(count.incrementAndGet());
                    }

                    dbConnectionPool.returnObject(dbConnection);

                    channel.basicAck(messageId, true);
                }


            };

            boolean autoAck = false;
            channel.basicConsume(MultiThreadedConsumer.QUEUE_NAME, autoAck, deliverCallback1, consumerTag -> {
            });

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
