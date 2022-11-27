

import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * A simple RabbitMQ channel pool based on a BlockingQueue implementation
 *
 */
public class DBConnectionPool {

    // used to store and distribute channels
    private final BlockingQueue<Connection> pool;
    // fixed size pool
    private int capacity;
    // used to ceate channels
    private DBConnectionFactory factory;


    public DBConnectionPool(int maxSize, DBConnectionFactory factory) {
        this.capacity = maxSize;
        pool = new LinkedBlockingQueue<>(capacity);
        this.factory = factory;
        for (int i = 0; i < capacity; i++) {
            Connection chan;
            try {
                chan = factory.create();
                pool.put(chan);
            } catch (IOException | InterruptedException ex) {
                Logger.getLogger(DBConnectionPool.class.getName()).log(Level.SEVERE, null, ex);
            }

        }
    }

    public Connection borrowObject()  {

        try {
            return pool.take();
        } catch (InterruptedException e) {
            throw new RuntimeException("Error: no channels available" + e.toString());
        }
    }

    public void returnObject(Connection connection)  {
        if (connection != null) {
            pool.add(connection);
        }
    }

    public void close() {
        // pool.close();
    }
}