/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 * @author Ian Gorton, Northeastern University
 * The examples supplement Chapter 7 of the Foundations of Scalable Systems, O'Reilly Media 2022
 */


import com.rabbitmq.client.Channel;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * A simple RabbitMQ channel factory based on the APche pooling libraries
 */
public class DBConnectionFactory extends BasePooledObjectFactory<Connection> {


    // used to count created channels for debugging
    private int count;

    public DBConnectionFactory() {
        count = 0;
    }

    @Override
    synchronized public Connection create() throws IOException {
        Connection connection = null;
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(String.format("jdbc:postgresql://%s:5432/skidb", System.getenv("DB_HOST")),
                    "efeleke", "password");
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return connection;

    }

    @Override
    public PooledObject<Connection> wrap(Connection channel) {
        //System.out.println("Wrapping channel");
        return new DefaultPooledObject<>(channel);
    }

    public int getChannelCount() {
        return count;
    }

    // for all other methods, the no-op implementation
    // in BasePooledObjectFactory will suffice
}