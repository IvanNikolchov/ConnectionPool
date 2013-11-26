/**
 * ConnectionPool.java
 *
 * @autor Ivan Nikolchov
 * 
 */
package org.nikolchov.db;

import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import org.apache.log4j.Logger;

/**
 *
 * @author ivan
 */
public class Connection {

  private java.sql.Connection connection;
  private boolean inUse;
  private final static int VALIDATION_TIMEOUT = 5000;
  private long timestamp;
  private final static Logger log = Logger.getLogger(Connection.class);

  Connection(java.sql.Connection connection) {
    this.connection = connection;
    timestamp = 0;
  }

  java.sql.Connection getConnection() {
    return connection;
  }

  boolean isInUse() {
    return inUse;
  }

  void setInUse() {
    synchronized (this) {
      if (inUse) {
        return;
      }
      inUse = true;
      timestamp = System.currentTimeMillis();
    }
  }

  long getLastUse() {
    return timestamp;
  }

  boolean isValid() {
    try {
      return connection.isValid(VALIDATION_TIMEOUT);
    }
    catch (SQLException ex) {
      return false;
    }
  }

  public void close() {
    inUse = false;
  }

  public PreparedStatement prepareStatement(String sql) throws SQLException {
    return connection.prepareStatement(sql);
  }

  public CallableStatement prepareCall(String sql) throws SQLException {
    return connection.prepareCall(sql);
  }

  public Statement createStatement() throws SQLException {
    return connection.createStatement();
  }

  public void setAutoCommit(boolean autoCommit) throws SQLException {
    connection.setAutoCommit(autoCommit);
  }

  public boolean getAutoCommit() throws SQLException {
    return connection.getAutoCommit();
  }

  public void commit() throws SQLException {
    connection.commit();
  }

  public void rollback() throws SQLException {
    connection.rollback();
  }

  public boolean isClosed() throws SQLException {
    return connection.isClosed();
  }

  public DatabaseMetaData getMetaData() throws SQLException {
    return connection.getMetaData();
  }

  public void setReadOnly(boolean readOnly) throws SQLException {
    connection.setReadOnly(readOnly);
  }

  public boolean isReadOnly() throws SQLException {
    return connection.isReadOnly();
  }

  public void setTransactionIsolation(int level) throws SQLException {
    connection.setTransactionIsolation(level);
  }

  public int getTransactionIsolation() throws SQLException {
    return connection.getTransactionIsolation();
  }

  public SQLWarning getWarnings() throws SQLException {
    return connection.getWarnings();
  }

  public void clearWarnings() throws SQLException {
    connection.clearWarnings();
  }

  /**
   * Executes CRUD queries, non-returning results.
   */
  public final void execQuery(String query, Object... params) throws SQLException {
    PreparedStatement statement = connection.prepareStatement(query);
    int cell = 1;
    for (int i = 0; i < params.length;) {
      int type = (Integer) params[i++];
      Object value = params[i++];
      setCell(type, value, statement, cell++);
    }
    statement.execute();
    statement.close();
  }

  /**
   * Executes queries returning result
   */
  final public Table getByQuery(String query, Object... params) throws SQLException {
    Table matrix = new Table();
    PreparedStatement statement = connection.prepareStatement(query);
    int cell = 1;
    for (int i = 0; i < params.length;) {
     int type = (Integer) params[i++];
      Object value = params[i++];
      setCell(type, value, statement, cell++);
    }
    ResultSet result = statement.executeQuery();
    ResultSetMetaData resultMetaData = result.getMetaData();
    while (result.next()) {
      ArrayList result_array = new ArrayList();
      for (int i = 1; i <= resultMetaData.getColumnCount(); i++) {
        result_array.add(getCell(resultMetaData.getColumnType(i), result, i));
      }
      matrix.add(result_array);
    }
    statement.close();
    result.close();
    return matrix;
  }

  /**
   * Executes queries, returns first generated key.
   */
  final public Integer execute(String query, Object... params) throws SQLException {
    PreparedStatement statement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
    int cell = 1;
    for (int i = 0; i < params.length;) {
      int type = (Integer) params[i++];
      Object value = params[i++];
      setCell(type, value, statement, cell++);
    }
    statement.executeUpdate();

    ResultSet rs = statement.getGeneratedKeys();
    rs.next();
    Integer insertedKeyValue = rs.getInt(1);
    rs.close();
    statement.close();
    return insertedKeyValue;
  }

  private Object getCell(int type, ResultSet rs, int indx) throws SQLException {
    switch (type) {
      case STRING:
      case TEXT:
        return rs.getString(indx);
      case INT:
        return rs.getInt(indx);
      case LONG:
        return rs.getLong(indx);
      case DOUBLE:
        return rs.getDouble(indx);
      case TIMESTAMP:
        Timestamp ts = rs.getTimestamp(indx);
        if (ts == null)
          return null;
        Calendar cal = GregorianCalendar.getInstance();
        cal.setTime(ts);
        return cal;
      case BOOL:
        return rs.getBoolean(indx);
      case DATE:
        Date date = rs.getDate(indx);
        if (date == null)
          return null;
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.setTime(date);
        return calendar;
      case BINARY:
        return rs.getBytes(indx); // byte[]
      default: // ??
        Object res = rs.getObject(indx);
        return res == null ? null : res.toString();
    }
  }

  private void setCell(int type, Object value, PreparedStatement statement, int idx) throws SQLException {
    if (value == null) {
      statement.setNull(idx, type);
      return;
    }
      
    switch (type) {
      case STRING:
      case TEXT:
        statement.setString(idx, (String) value);
        break;
      case INT:
        statement.setInt(idx, (Integer) value);
        break;
      case DOUBLE:
        statement.setDouble(idx, (Double) value);
        break;
      case BOOL:
        statement.setBoolean(idx, (Boolean) value);
        break;
      case TIMESTAMP:
      case DATE: {
        Calendar cal = (Calendar) value;
        long date_milis = cal.getTimeInMillis();
        Timestamp ts = new Timestamp(date_milis);
        statement.setTimestamp(idx, ts, cal);
        break;
      }
      case LONG:
        statement.setLong(idx, (Long) value);
        break;
      case BINARY:
        statement.setBytes(idx, (byte[]) value);
        break;
    }
  }
  public final static int STRING = Types.VARCHAR;
  public final static int TEXT = Types.LONGVARCHAR;
  public final static int INT = Types.INTEGER;
  public final static int DOUBLE = Types.DOUBLE;
  public final static int BOOL = Types.BOOLEAN;
  public final static int TIMESTAMP = Types.TIMESTAMP;
  public final static int DATE = Types.DATE;
  public final static int LONG = Types.BIGINT;
  public final static int BINARY = Types.BINARY;
}
