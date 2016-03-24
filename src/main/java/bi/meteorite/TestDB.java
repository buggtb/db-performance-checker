package bi.meteorite;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static java.sql.DriverManager.getConnection;

/**
 * Created by bugg on 22/03/16.
 */
public class TestDB {




  public TestDB(String configpath, String querypath, String concurrency, String validateresult) {

    ArrayList<Future<Map<String, Object>>> list;
    ExecutorService service = null;


    try {
      service = Executors.newFixedThreadPool(Integer.parseInt(concurrency));

    }
    catch(NumberFormatException e){
      service = Executors.newFixedThreadPool(50);
    }


    Properties configprops = loadConfig(configpath);

    Connection sourceconnection =
        connectDB(configprops.getProperty("sourcedb.url"), configprops.getProperty("sourcedb.username"),
            configprops.getProperty("sourcedb.password"),
            configprops.getProperty("sourcedb.class"));

    Connection targetconnection =
        connectDB(configprops.getProperty("targetdb.url"), configprops.getProperty("targetdb.username"),
            configprops.getProperty("targetdb.password"),
            configprops.getProperty("targetdb.class"));

    for(int i=0;i<100;i++) {
      list=new ArrayList();
      Properties[] files = loadFiles(concurrency, querypath, i);

      System.out.println("Executing batch "+i);
      if(files.length==0){
        break;
      }
      for(Properties p: files) {
        if (files.length > 0) {
          Future<Map<String, Object>> future = service.submit(new QueryExecutor("Source Database", sourceconnection,
              p.getProperty("sourcedb.query")));
          list.add(future);

          future = service.submit(new QueryExecutor("Target Database", targetconnection,
              p.getProperty("targetdb.query")));
          list.add(future);
        } else {
          break;
        }

      }

      ResultSet sourceResultSet = null;
      ResultSet targetResultSet = null;
      Object targetOutput = null;
      Object sourceOutput = null;
      for (int x = 0; x < list.size(); x++) {
        Future<Map<String, Object>> fut = list.get(x);
        try {
          //print the return value of Future, notice the output delay in console
          // because Future.get() waits for task to get completed
          if (validateresult.equals("true")) {
            if ((x & 1) == 0) {
              targetResultSet = (ResultSet) fut.get().get("resultset");
              targetOutput = fut.get().get("output");
            } else {
              sourceResultSet = (ResultSet) fut.get().get("resultset");
              sourceOutput = fut.get().get("output");
            }

            if (targetResultSet != null && sourceResultSet != null) {
              System.out
                  .println(new Date() + "::" + sourceOutput);
              System.out
                  .println(new Date() + "::" + targetOutput);
              boolean ans = compareResultSets(sourceResultSet, targetResultSet);
              System.out
                  .println("Resultsets matched? " + String.valueOf(ans));
              sourceResultSet = null;
              targetResultSet = null;
            }
          } else {
            System.out.println(new Date() + "::" + fut.get().get("output"));
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      System.out.println("Batch " +i+" complete");
      if(concurrency.equals("all")){
        break;
      }
    }
    //shut down the executor service now
    service.shutdown();

  }

  private Properties loadConfig(String configpath){
    Properties prop = new Properties();
    InputStream input;

    try {

      input = new FileInputStream(configpath);

      // load a properties file
      prop.load(input);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return prop;
  }

  private Connection connectDB(String url, String username, String password, String classname){
    try {
      Class.forName(classname);

      return getConnection(url, username, password);
      //Statement stmt = conn.createStatement();
      //ResultSet rs;

      //rs = stmt.executeQuery("SELECT Lname FROM Customers WHERE Snum = 2001");
      //while ( rs.next() ) {
       // String lastName = rs.getString("Lname");
      //  System.out.println(lastName);
      //}
      //conn.close();
    } catch (Exception e) {
      System.err.println("Got an exception! ");
      System.err.println(e.getMessage());
    }

    return null;
  }

  public File[] finder(String dirName){
    File dir = new File(dirName);

    return dir.listFiles(new FilenameFilter() {
      public boolean accept(File dir, String filename)
      { return filename.endsWith(".properties"); }
    } );

  }

  private Properties[] loadFiles(String concurrency, String querypath, int batchno) {
    File[] files = finder(querypath);

    ArrayList<File> filesreduced = new ArrayList();
    ArrayList<Properties> props = new ArrayList();

    if(!concurrency.equals("all")){
      for(int j = 0; j<files.length; j++){
        if(j<(Integer.parseInt(concurrency)*batchno)){

        }
        else{
          filesreduced.add(files[j]);

        }

      }

      if(filesreduced.size()>0){
        List<File> contestWinners;
        if(filesreduced.size()>=Integer.parseInt(concurrency)) {
          contestWinners = filesreduced.subList(0, Integer.parseInt(concurrency));

        }
        else{
          contestWinners = filesreduced;
        }

      for(File f: contestWinners) {
        Properties prop = new Properties();
        InputStream input;

        try {
          input = new FileInputStream(f);
          prop.load(input);

        } catch (IOException e) {
          e.printStackTrace();
        }

        props.add(prop);
      }
      }

    }
    else{
      for(File f:files) {
        Properties prop = new Properties();
        InputStream input;

        try {
          input = new FileInputStream(f);
          prop.load(input);

        } catch (IOException e) {
          e.printStackTrace();
        }

        props.add(prop);
      }
    }

    return props.toArray(new Properties[props.size()]);
  }

  public static boolean compareResultSets(ResultSet rs1, ResultSet rs2)
  {

    boolean result = true;
    boolean dataExists = false;

    try
    {

      ResultSetMetaData rsMd1 = rs1.getMetaData();
      ResultSetMetaData rsMd2 = rs2.getMetaData();

      int numberOfColumns1 = rsMd1.getColumnCount();
      int numberOfColumns2 = rsMd2.getColumnCount();
      if(!(numberOfColumns1 == numberOfColumns2))
        return false;

      while (rs1.next() && rs2.next())
      {
        dataExists = true;
        //System.out.println("Row " + rowCount + ":  ");


        for(int columnCount = 1; columnCount <= numberOfColumns1; columnCount++)
        {

          String columnName = rsMd1.getColumnName(columnCount);
          int columnType = rsMd1.getColumnType(columnCount);

          if(columnType == Types.CHAR || columnType == Types.VARCHAR || columnType ==

                                                                        Types.LONGVARCHAR)
          {
            String columnValue1 = rs1.getString(columnCount);
            String columnValue2 = rs2.getString(columnCount);
            if(columnValue1==null && columnValue2 == null){

            }
            else if((columnValue1 == null) || (columnValue2 == null) || !(columnValue1.equals(
                columnValue2)))
              result = false;
          }
          else if(columnType == Types.INTEGER || columnType == Types.BIGINT || columnType ==

                                                                               Types.SMALLINT || columnType == Types.NUMERIC)
          {
            Long columnValue1 = rs1.getLong(columnCount);
            Long columnValue2 = rs2.getLong(columnCount);
            if(!(columnValue1.equals(columnValue2)))
              result = false;

          }
          else if(columnType == Types.DECIMAL || columnType == Types.DOUBLE || columnType ==

                                                                               Types.FLOAT || columnType == Types.REAL)
          {

          }
          else if(columnType == Types.TIME || columnType == Types.TIMESTAMP || columnType ==

                                                                               Types.DATE)
          {
            Timestamp columnValue1 = rs1.getTimestamp(columnCount);
            Timestamp columnValue2 = rs2.getTimestamp(columnCount);
            if(!(columnValue1.equals(columnValue2)))
              result = false;


          }
          // System.out.println("Column Name: "+columnName+ " Column Type: "+columnType + "  Column

//  Value: "+ columnValue + " "+rsmd1.getColumnClassName(columnCount));
        }


      }

      if(rs1.next() && !rs2.next()){
        result=false;
      }
      else if(!rs1.next() && rs2.next()){
        result=false;
      }

    } catch(Exception e)
    {
      System.out.println(e.getLocalizedMessage());

    }
    //System.out.println(result);
    return result && dataExists;
  }

}



