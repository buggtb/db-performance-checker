package bi.meteorite;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Created by bugg on 22/03/16.
 */
public class QueryExecutor implements Callable{

  private final Connection conn;
  private final String query;
  private final String name;

  public QueryExecutor(String name, Connection connection, String query) {
    this.conn = connection;
    this.query = query;
    this.name = name;
  }

  public Object call() throws Exception {

    long startTime = System.currentTimeMillis();


    Statement stmt = null;
    try {
      stmt = conn.createStatement();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    ResultSet rs = null;

    try {
      if (stmt != null) {
        rs = stmt.executeQuery(query);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }

    long stopTime = System.currentTimeMillis();


    long seconds = TimeUnit.MILLISECONDS.toSeconds(stopTime - startTime);
    HashMap<String, Object> hashMap = new HashMap();
    hashMap.put("name", name);
    DecimalFormat df = new DecimalFormat("#.####");
    df.setRoundingMode(RoundingMode.CEILING);
    hashMap.put("output", "Database: "+name+" took "+ String.valueOf(df.format(seconds))+" executing query: "+query);
    hashMap.put("resultset", rs);
    return hashMap;

  }

}
