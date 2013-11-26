/**
 * ConnectionPool.java
 *
 * @autor Ivan Nikolchov
 * 
 */
package org.nikolchov.db;

import java.sql.*;
import java.util.ArrayList;
import org.apache.log4j.Logger;
import org.libra.webdialer.db.Connection;
import org.libra.webdialer.misc.AppOptions;
import org.libra.webdialer.misc.Constants;

public class ConnectionPool {

  private class PoolManager extends Thread {

    private ConnectionPool pool;
    private boolean active = false;
    private final long delay = 60000000; // 10 min?

    private PoolManager(ConnectionPool pool) {
      this.pool = pool;
    }

    @Override
    public void run() {
      while (active) {
        try {
          sleep(delay);
        }
        catch (InterruptedException e) {}
        if (active) {
          pool.validateConnections();
        }
      }
      log.info("Connection pool manager stopped");
    }

    private void stopManager() {
      log.info("Stoping connection pool manager...");
      active = false;
      interrupt();
    }
    
    private void startManager() {
      active = true;
      start();
    }
  }
  private final static Object mutex = new Object();
  private static final Logger log = Logger.getLogger(ConnectionPool.class);
  private final static long TIMEOUT = 6000000;
  private static ConnectionPool pool = null;
  private final PoolManager manager;
  private ArrayList<Connection> connections;
  private final String CONNECTION_STRING;

  private ConnectionPool(AppOptions options) {
    try {
      Class.forName(Constants.JDBC_DRIVER_CLASS);
    }
    catch (ClassNotFoundException cnfe) {
      log.error("Couldn't find the driver!", cnfe);
    }
    
    CONNECTION_STRING = String.format(Constants.DB_URL, options.getSqlServer(), 
                                      options.getSqlServerPort(), options.getDbName());
    connections = new ArrayList<Connection>(options.getConnPoolSize());
    manager = new PoolManager(this);
  }

  private void validateConnections() {
    synchronized (mutex) {
      long timestamp = System.currentTimeMillis() - TIMEOUT;
      for (int i = 0; i < pool.connections.size(); ++i) {
        Connection connection = pool.connections.get(i);
        if (connection.isInUse() && timestamp > connection.getLastUse() || !connection.isValid()) {
          log.error("REMOVED CONNECTION " + i);
          connections.remove(i--);
          java.sql.Connection conn = connection.getConnection();
          try {
            if (!conn.getAutoCommit())
              conn.rollback();
            conn.close();
          }
          catch (SQLException ex) {
            log.error("Unable to close connection!", ex);
          }
        }
      }
    }
  }

  public static void create() throws SQLException {
    synchronized (mutex) {
      if (pool != null) {
        return;
      }
      AppOptions options = AppOptions.getOptions();
      pool = new ConnectionPool(options);
      for (int i = 0; i < options.getConnPoolSize(); ++i) {
        java.sql.Connection conn = DriverManager.getConnection(pool.CONNECTION_STRING, options.getSqlUser(), options.getSqlPass());
        Connection connection = new Connection(conn);
        connection.setAutoCommit(false);
        pool.connections.add(connection);
      }
      pool.manager.setName("Connection Pool Manager");
      pool.manager.startManager();
    }
  }

  public static void destroy() {
    pool.manager.stopManager();
    for (int i = 0; i < pool.connections.size(); ++i) {
      try {
        pool.connections.get(i).getConnection().close();
      }
      catch(Throwable t) {
      }
    }
    pool = null;
    log.info("Connection pool destroyed!");
  }

  public static Connection getConnection() throws SQLException {
    synchronized (mutex) {
      for (Connection connection : pool.connections) {
        if (!connection.isInUse() && connection.isValid()) {
          connection.setInUse();
          return connection;
        }
      }
      AppOptions options = AppOptions.getOptions();
      java.sql.Connection conn = DriverManager.getConnection(pool.CONNECTION_STRING, options.getSqlUser(), options.getSqlPass());
      log.info("New Connection Created!");
      log.info("All connections: " + pool.connections.size());
      Connection connection = new Connection(conn);
      pool.connections.add(connection);
      connection.setAutoCommit(false);
      connection.setInUse();
      return connection;
    }
  }
}
