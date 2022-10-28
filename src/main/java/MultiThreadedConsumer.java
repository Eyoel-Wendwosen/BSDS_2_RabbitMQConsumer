import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

public class MultiThreadedConsumer {
    public static final String QUEUE_NAME = "ski_event";

    public static void main(String[] args) throws IOException, TimeoutException {
        if (args.length != 4) {
            System.out.printf("Error: Running the following program requires three arguments \njava rabbitmq.jar <hostname> <username> <password> <num of threads>\n");
        }
        ConnectionFactory factory = new ConnectionFactory();
        int numOfThreads = 1;
        if (args[0].equals("localhost")) {
            factory.setHost("localhost");
        } else {
            factory.setHost(args[0]);
            factory.setUsername(args[1]);
            factory.setPassword(args[2]);
            numOfThreads = Integer.parseInt(args[3]);
        }

        Connection connection = factory.newConnection();
        ConcurrentHashMap<String, SkiEvent> skiEventConcurrentHashMap = new ConcurrentHashMap<>();
        for (int i = 0; i < numOfThreads; i++) {
            Consumer consumer = new Consumer(connection, skiEventConcurrentHashMap);
            Thread thread = new Thread(consumer);
            thread.start();
        }

    }

}
