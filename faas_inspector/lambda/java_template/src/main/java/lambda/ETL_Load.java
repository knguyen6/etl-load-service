/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context; 
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import faasinspector.register;
import java.io.File;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
/**
 * uwt.lambda_test::handleRequest
 * @author wlloyd
 */
public class ETL_Load implements RequestHandler<Request, Response>
{
    static String CONTAINER_ID = "/tmp/container-id";
    static Charset CHARSET = Charset.forName("US-ASCII");
    
    
    // Lambda Function Handler
    public Response handleRequest(Request request, Context context) {
        //TODO: lambda trigger by new object in S3, get object key
        String bucketName = System.getenv("BUCKET_NAME");;

        // Create logger
        LambdaLogger logger = context.getLogger();
        
        // Register function
        register reg = new register(logger);

        //stamp container with uuid
        Response r = reg.StampContainer();
        int uses = 0;

        setCurrentDirectory("/tmp");
        try
        {
            String tablename = "etlload";

            // Connection string an in-memory SQLite DB
            Connection con = DriverManager.getConnection("jdbc:sqlite:");

            // Connection string for a file-based SQlite DB
//            Connection con = DriverManager.getConnection("jdbc:sqlite:mytest.db");
            
            // Detect if the table 'mytable' exists in the database
            PreparedStatement ps = con.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name='" + tablename + "'");
            ResultSet rs = ps.executeQuery();
            if (!rs.next())
            {
                // 'mytable' does not exist, and should be created
                logger.log("trying to create table '" + tablename + "'");
                ps = con.prepareStatement("CREATE TABLE mytable ( name text, col2 text, col3 text);");
                ps.execute();
            }
            rs.close();
            
            // Insert row into mytable
            ps = con.prepareStatement("insert into mytable values('" + request.getName() + "','b','c');");
            ps.execute();
            
            // Query mytable to obtain full resultset
            ps = con.prepareStatement("select * from mytable;");
            rs = ps.executeQuery();
            // Load query results for [name] column into a Java Linked List
            // ignore [col2] and [col3] 
            LinkedList<String> ll = new LinkedList<String>();
            while (rs.next())
            {
                logger.log("name=" + rs.getString("name"));
                ll.add(rs.getString("name"));
                logger.log("col2=" + rs.getString("col2"));
                logger.log("col3=" + rs.getString("col3"));
            }
            rs.close();
            con.close();
            r.setNames(ll);
        }
        catch (SQLException sqle)
        {
            logger.log("DB ERROR:" + sqle.toString());
            sqle.printStackTrace();
        }
        
        
        // *********************************************************************
        // Implement Lambda Function Here
        // *********************************************************************
        uses = uses + 1;
        String hello = "Hello " + request.getName() + " calls = " + uses;

        
        // Set return result in Response class, class is marshalled into JSON
        r.setValue(hello);
        
        return r;
    }
    
    private static boolean setCurrentDirectory(String directory_name)
    {
        boolean result = false;  // Boolean indicating whether directory was set
        File    directory;       // Desired current working directory

        directory = new File(directory_name).getAbsoluteFile();
        if (directory.exists() || directory.mkdirs())
        {
            result = (System.setProperty("user.dir", directory.getAbsolutePath()) != null);
        }

        return result;
    }

    //TODO: get file from s3
    private void getDataFromS3(String bucketName, String objectKey) {

    }

    //TODO: upload file to s3
    private void putFileToS3(String bucketName, String objectKey){

    }

    //TODO: after getting csv file from s3, read line by line and insert to Db
    private void csvToDb(String fileName){

    }

    //TODO: from local db, export as csv and upload to s3
    private String sqlToCsv(String dbTableName, String csvFileName){
        //From dbTableName, export to csv as csvFileName

        return "";//path to csvFileName under /tmp

    }
    
    // int main enables testing function from cmd line
    public static void main (String[] args)
    {
        Context c = new Context() {
            @Override
            public String getAwsRequestId() {
                return "";
            }

            @Override
            public String getLogGroupName() {
                return "";
            }

            @Override
            public String getLogStreamName() {
                return "";
            }

            @Override
            public String getFunctionName() {
                return "";
            }

            @Override
            public String getFunctionVersion() {
                return "";
            }

            @Override
            public String getInvokedFunctionArn() {
                return "";
            }

            @Override
            public CognitoIdentity getIdentity() {
                return null;
            }

            @Override
            public ClientContext getClientContext() {
                return null;
            }

            @Override
            public int getRemainingTimeInMillis() {
                return 0;
            }

            @Override
            public int getMemoryLimitInMB() {
                return 0;
            }

            @Override
            public LambdaLogger getLogger() {
                return new LambdaLogger() {
                    @Override
                    public void log(String string) {
                        System.out.println("LOG:" + string);
                    }
                };
            }
        };
        
        // Create an instance of the class
        ETL_Load lt = new ETL_Load();
        
        // Create a request object
        Request req = new Request();
        
        // Grab the name from the cmdline from arg 0
        String name = (args.length > 0 ? args[0] : "");
        
        // Load the name into the request object
        req.setName(name);

        // Report name to stdout
        System.out.println("cmd-line param name=" + req.getName());
        
        // Run the function
        Response resp = lt.handleRequest(req, c);
        try
        {
            Thread.sleep(100000);
        }
        catch (InterruptedException ie)
        {
            System.out.print(ie.toString());
        }
        // Print out function result
        System.out.println("function result:" + resp.toString());
    }
}
