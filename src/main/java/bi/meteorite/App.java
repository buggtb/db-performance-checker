package bi.meteorite;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Options options = new Options();
        options.addOption("config", true, "Path to connection configuration");

        options.addOption("concurrency", true, "Number of concurrent queries to run");

        options.addOption("querypath", true, "Path to the defined queries");

        options.addOption("validateresult", "Ensure the resultsets match");


        CommandLineParser parser = new BasicParser();
        CommandLine cmd = null;
        try{
            cmd = parser.parse(options, args);
        }catch (ParseException pe){ usage(options);
        }

        String configpath = null;
        String querypath = null;
        String validateresult;
        String concurrency;
        if (cmd != null) {
            if(cmd.hasOption("config")) {
                configpath = cmd.getOptionValue("config");
            }
            else{
                System.err.println("You need to provide a path to the config file.");
                System.exit(1);

            }

            if(cmd.hasOption("querypath")){
                querypath = cmd.getOptionValue("querypath");
            }
            else{
                System.err.println("You need to provide a path to the the queries.");
                System.exit(1);
            }

            concurrency = cmd.getOptionValue("concurrency", "1");

            if(cmd.hasOption("validateresult")) {
                validateresult = "true";
            }
            else{
                validateresult = "false";
            }

            TestDB tdb = new TestDB(configpath, querypath, concurrency, validateresult);
        }


    }


    private static void usage(Options options){

// Use the inbuilt formatter class
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp( "DB Checker", options );
    }

}
