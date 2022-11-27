import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiThreadedConsumer {
    public static final String QUEUE_NAME = "skiEvent";

    public static void main(String[] args) throws IOException, TimeoutException {
        if (args.length != 4) {
            System.out.printf("Error: Running the following program requires three arguments \njava rabbitmq.jar <hostname> <username> <password> <num of threads>\n");
        }
        ConnectionFactory factory = new ConnectionFactory();

        factory.setHost(System.getenv("RABBIT_MQ"));
        factory.setUsername("test");
        factory.setPassword("test");
        int numOfThreads = Integer.parseInt(args[0]);


        DBConnectionFactory dbConnectionFactory = new DBConnectionFactory();
        DBConnectionPool dbConnectionPool = new DBConnectionPool(numOfThreads, dbConnectionFactory);

        createTables(dbConnectionPool);


        Connection connection = factory.newConnection();
        AtomicInteger count = new AtomicInteger(0);
        for (int i = 0; i < numOfThreads; i++) {
            Consumer consumer = new Consumer(connection, dbConnectionPool, count);
            Thread thread = new Thread(consumer);
            thread.start();
        }

    }

    private static void createTables(DBConnectionPool dbConnectionPool) {
        java.sql.Connection connection = dbConnectionPool.borrowObject();
        Statement stmt = null;
        try {
            connection.setAutoCommit(false);
            stmt = connection.createStatement();

            String sql = "CREATE TABLE IF NOT EXISTS  skier " +
                    "(id varchar(50) PRIMARY KEY     NOT NULL," +
                    " name varchar(100)    NOT NULL, " +
                    " age INT NOT NULL, " +
                    " skier_id INT NOT NULL UNIQUE);" +
                    "CREATE TABLE IF NOT EXISTS resort " +
                    "(id VARCHAR(50) PRIMARY KEY     NOT NULL," +
                    " name VARCHAR(100)    NOT NULL, " +
                    " address VARCHAR(100) NOT NULL, " +
                    " resort_id INT NOT NULL UNIQUE );" +
                    "CREATE TABLE IF NOT EXISTS skievent " +
                    "(id varchar(50) PRIMARY KEY     NOT NULL," +
                    " day_id INT NOT NULL, " +
                    " lift_id INT NOT NULL, " +
                    " season_id INT NOT NULL, " +
                    " skier_id VARCHAR NOT NULL REFERENCES skier(id), " +
                    " resort_id VARCHAR NOT NULL REFERENCES resort(id), " +
                    " time INT NOT NULL )";

            stmt.executeUpdate(sql);

            stmt.close();
            connection.commit();

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        dbConnectionPool.returnObject(connection);
    }

}
