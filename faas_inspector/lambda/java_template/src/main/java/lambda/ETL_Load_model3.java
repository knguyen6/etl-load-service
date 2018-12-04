/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;

import java.io.File;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.model.PutObjectRequest;
import faasinspector.register;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * uwt.lambda_test::handleRequest
 * @author wlloyd
 */
public class ETL_Load_3 implements RequestHandler<Request, Response>
{
    static String CONTAINER_ID = "/tmp/container-id";
    static Charset CHARSET = Charset.forName("US-ASCII");
    LambdaLogger logger = null;
    private AmazonS3 s3client;
    private static final String LAMBDA_TEMP_DIRECTORY = "/tmp/";
    private static final String AWS_REGION = "us-east-1";
    private static final String TRANSFORM = "transformed"; //after lambda 1 transform, put csv here.
    private static final String LOAD = "loaded"; // after lambda 2 (this) load db, upload db file here.
    private static final String CSV_DELIM = ",";

    // Lambda Function Handler
    public Response handleRequest(Request request, Context context) {
        String bucketName = request.getBucketname(); //get bucketname from req.
        String fileName = request.getFilename(); //get export_filename from req.


        String dbName = System.getenv("DB_NAME");
        String tableName = System.getenv("TABLE_NAME");

        //static path to csv file under /tmp to be loaded to db table:
        String csvFilePath = LAMBDA_TEMP_DIRECTORY + fileName;

        // Create logger
        logger = context.getLogger();

        //setup S3 client :
        s3client = AmazonS3ClientBuilder.standard().withRegion(AWS_REGION).build();

        // Register function
        register reg = new register(logger);

        //stamp container with uuid
        Response r = reg.StampContainer();

        //make sure to have  "bucketname" and "filename" in request obj:
        if (bucketName == null || fileName == null){
            setResponseObj(r, false, "\"bucketname\" and \"filename\" are required in request obj", null, null, null);
            return r;
        }

        // make sure to have environment variables set for DB_NAME and TABLE_NAME:
        if (dbName == null || tableName == null){
            setResponseObj(r, false, " Environment variables: \"DB_NAME\" or \"TABLE_NAME\" are not set", null, null, null);
            return r;
        }


        logger.log("bucketname = " + bucketName + ", filename = " + fileName);

        setCurrentDirectory("/tmp");

        // *********************************************************************
        // Implement Lambda Function Here
        // *********************************************************************

        try
        {
            // get file from s3, download to /tmp
            File tmpDir = new File(csvFilePath);
            boolean exists = tmpDir.exists();
            if (!exists) {
                getDataFromS3(bucketName, fileName);

            }

            // Connection string for a file-based SQlite DB
            Connection con = DriverManager.getConnection("jdbc:sqlite:" + dbName);

            // Detect if the table  exists in the database
            PreparedStatement ps = con.prepareStatement("SELECT * FROM sqlite_master WHERE type='table' AND name='" + tableName + "'");
            ResultSet rs = ps.executeQuery();

            // if table doesnt exist, create new one:
            if (!rs.next())
            {
                // table does not exist, and should be created
                logger.log(" Table doesnt exist. Creating table: '" + tableName + "'");

                ps = con.prepareStatement("CREATE TABLE "+ tableName+"( \"Order ID\" TEXT PRIMARY KEY, \"Region\" TEXT,\"Country\" TEXT,\"Item Type\" TEXT,\"Sales Channel\" TEXT,\"Order Priority\" TEXT,\"Order Date\" TEXT,\"Ship Date\" TEXT,\"Units Sold\" TEXT,\"Unit Price\" TEXT,\"Unit Cost\" TEXT,\"Total Revenue\" TEXT,\"Total Cost\" TEXT,\"Total Profit\" TEXT);");
                ps.execute();
                rs.close();

                //insert data from csv file to table:
                insertTable(csvFilePath, tableName, con);

                ps = con.prepareStatement("select * from "+ tableName+ ";");
                rs = ps.executeQuery();
                // Debugging, print out result from select statement:
//                displayData(rs);

            }
            else {
                logger.log("Database \'" + dbName + "' exists with table: " + tableName
                        + ", no new table needs to be created, upload cached table to s3.");
            }

            // need to upload db to s3
            putFileToS3(bucketName, new File(LAMBDA_TEMP_DIRECTORY + dbName));
            setResponseObj(r, true, null, bucketName, dbName, tableName);

            rs.close();
            con.close();

            logger.log("closing all connection !!!");

        }
        catch (SQLException sqle)
        {
            logger.log("DB ERROR:" + sqle.toString());
            setResponseObj(r, false, sqle.toString(), null, null, null);
        }
        catch (Exception e) {
            logger.log("File Error: " + e.toString());
            setResponseObj(r, false, e.toString(), null, null, null);
        }

        return r;
    }

    // set response obj
    private void setResponseObj(Response r, boolean success, String e, String bucketName,
                                String dbName, String tableName) {
        // Set response object:
        if (success) {
            r.setSuccess(true);
            r.setBucketname(bucketName);
            r.setDbname(dbName);
            r.setTablename(tableName);
        }
        else {
            r.setSuccess(false);
            r.setError(e.toString());
        }

    }

    //insert data from csv to table
    private static void insertTable(String csvFilePath, String tablename, Connection con) {

        //try inserting data to table from local csv:
        String line = "";
        int lineCount = 0;
        try{
            BufferedReader br = new BufferedReader(new FileReader(csvFilePath));
            while ((line = br.readLine()) != null) {
                lineCount++;

                //skip first line, it's headers, no need to insert
                if (lineCount == 1) {
                    continue;
                }

                String[] country = line.split(CSV_DELIM);
                String values = "";

                //build the values to insert, needs single quotes per string, and comma in b/w:
                for (int i = 0; i < country.length; i++) {
                    values = values + "'" + country[i] + "'";

                    //add comma after every value except the last value
                    if (i != country.length - 1)
                        values = values + CSV_DELIM;
                } //for

                //insert each row to table:
                PreparedStatement ps = con.prepareStatement("insert into " + tablename + " values(" + values + ");");
                ps.execute();
            }//while
        }
        catch(Exception e ){
            System.out.println("Error inserting data (from csv) to table. " + e.getMessage());
            throw new IllegalArgumentException("Error inserting data (from csv) to table. " + e.getMessage());

        }
    }

    /**
     * Helper method
     * @param directory_name
     * @return boolean
     */
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

    /**
     * Helper func to display data from a result set.
     * @param resultSet
     */
    private void displayData(ResultSet resultSet) {
        logger.log("Displaying result set: ");
        try {
            ResultSetMetaData rsmd = resultSet.getMetaData();

            int columnsNumber = rsmd.getColumnCount();
            while (resultSet.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = resultSet.getString(i);
                    System.out.print(columnValue + "-" + rsmd.getColumnName(i));
                }
                System.out.println("");
            }
        }
        catch (SQLException e) {
            System.out.println("Error displayData. " + e.getMessage());
        }

    }

    /**
     *  get s3 object
     * @param bucketName bucket name
     * @param objectKey object filename aka name of file in s3, or path of file in s3
     */
    private void getDataFromS3(String bucketName, String objectKey) {
        System.out.println("getting file from s3: " + bucketName + " : " + TRANSFORM + "/" + objectKey);
        try {
            s3client.getObject(new GetObjectRequest(bucketName, TRANSFORM + "/" + objectKey),
                    new File(LAMBDA_TEMP_DIRECTORY + objectKey));
        }
        catch (Exception e) {
            logger.log("Error getting object from S3. " + e.getMessage());
            throw new IllegalArgumentException("getDataFromS3() Failed to get object from s3: " + e.getMessage());

        }


    }

    //list File under a path:
    public static void listFile(String path){
        System.out.println("========== listFile under: " + path);
        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles.length > 0 ) {
            for (int i = 0; i < listOfFiles.length; i++) {
                if (listOfFiles[i].isFile()) {
                    System.out.println("File " + listOfFiles[i].getName());

                } else if (listOfFiles[i].isDirectory()) {
                    System.out.println("Directory " + listOfFiles[i].getName());
                }
            }
        }

        System.out.println("=============================================");
    }

    /**
     * Upload a file to S3.
     * @param bucketName name of bucket
     * @param file to upload to S3
     */
    private void putFileToS3(String bucketName, File file){
        System.out.println("uploading file to s3 for: " + file.getPath() + ", name: " + file.getName());
        String objectKey = LOAD + "/" + file.getName();

        try {
            //create an object with data will be used by s3client to upload:
            PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, objectKey, file);
            s3client.putObject(putObjectRequest);//upload to S3
            logger.log("uploadFileToS3 to " + bucketName + ". Successfully uploaded " + file.getName());
        }
        catch (Exception ase){
            logger.log("Failed to upload to S3 at bucketName: " + bucketName +
                    ", objectKey: " + objectKey + ", Exception: " +ase.toString());
            throw new IllegalArgumentException("putFileToS3() Failed to upload to s3: " + ase.toString());
        }
    }


    // TODO: fix this so we can collect metrics:
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

//        // Create an instance of the class
//        ETL_Load lt = new ETL_Load();
//
//        // Create a request object
//        Request req = new Request();
//        System.out.println("");
//
//        //TODO: fix this
//        // Grab the name from the cmdline from arg 0
//        String filename = (args.length > 0 ? args[0] : "");
//
//        // Load the name into the request object
//        req.setBucketname(filename);
//
//        // Run the function
//        Response resp = lt.handleRequest(req, c);
//        try
//        {
//            Thread.sleep(100000);
//        }
//        catch (InterruptedException ie)
//        {
//            System.out.print(ie.toString());
//        }
//        // Print out function result
//        System.out.println("function result:" + resp.toString());
    }
}
