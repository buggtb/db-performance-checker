package bi.meteorite;

import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static java.sql.DriverManager.getConnection;

/**
 * Created by bugg on 22/03/16.
 */
public class QueryExecutor implements Callable{

  private final String query;
  private final String name;
  private final Properties configprops;
  private final String propprepend;

  public QueryExecutor(String name, Properties connection, String query, String target) {
    this.configprops = connection;
    this.query = query;
    this.name = name;
    this.propprepend = target;
  }

  public Object call() throws Exception {

    System.out.println("connecting to "+ propprepend+ " dataabase");

    Connection targetconnection =
        connectDB(configprops.getProperty(propprepend+".url"), configprops.getProperty(propprepend+".username"),
            configprops.getProperty(propprepend+".password"),
            configprops.getProperty(propprepend+".class"));

    long startTime = System.currentTimeMillis();


    System.out.println(propprepend+ "creating statement");
    Statement stmt = null;
    try {
      stmt = targetconnection.createStatement();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    ResultSet rs = null;

    System.out.println(propprepend+ " executing query");
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
    hashMap.put("output", "Database: "+name+" took "+ String.valueOf(df.format(seconds))+" seconds executing query: "
                          + ""+query);
    hashMap.put("resultset", rs);
    System.out.println(propprepend+ " closing connection");
    targetconnection.close();
    return hashMap;


  }


  private Connection connectDB(String url, String username, String password, String classname){
    try {
      Class.forName(classname);

      return getConnection(url, username, password);
    } catch (Exception e) {
      System.err.println("Got an exception! ");
      System.err.println(e.getMessage());
    }

    return null;
  }

}
