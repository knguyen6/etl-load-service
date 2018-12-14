/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;
import com.amazonaws.services.lambda.AWSLambda;
import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.model.InvokeResult;
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
import org.apache.http.HttpStatus;
import org.json.JSONException;
import org.json.JSONObject;

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
public class ETL_Load implements RequestHandler<Request, Response>
{
    static String CONTAINER_ID = "/tmp/container-id";
    static Charset CHARSET = Charset.forName("US-ASCII");
    LambdaLogger logger = null;
    private AmazonS3 s3client;
    private AWSLambda lambdaClient;


    private static final String LAMBDA_TEMP_DIRECTORY = "/tmp/";
    private static final String AWS_REGION = "us-east-1";
    private static final String TRANSFORM = "transformed"; //after lambda 1 transform, put csv here.
    private static final String LOAD = "loaded"; // after lambda 2 (this) load db, upload db file here.
    private static final String CSV_DELIM = ",";

    private static final String bucketName = System.getenv("BUCKET_NAME");
    private static final String tableName = System.getenv("TABLE_NAME");
    private static final String invokedLambdaName = System.getenv("EXTRACTION_LAMBDA_NAME");

    // Lambda Function Handler, no need to invoke another lambda
    public Response handleRequest(Request request, Context context) {
        // Create logger
        logger = context.getLogger();

        // Register function
        register reg = new register(logger);

        //stamp container with uuid
        Response r = reg.StampContainer();

        setCurrentDirectory("/tmp");
        String fileName = request.getExported_filename() != null ? request.getExported_filename() : request.getFilename(); //get exported_filename from req.
        String transactionId = request.getTransactionid(); //get transactionId from req.

        //make sure env vars and required field in request present:
        String precheckErrMsg = validateParams(request);
        if (precheckErrMsg != null) {
            logger.log("Something is missing: " + precheckErrMsg);
            setResponseObj(r, false, transactionId,precheckErrMsg, null, null, null);
            return r;
        }

        //parsing out the number from file name, to use for db name:
        String numbers = fileName.replaceAll("[^0-9]", "");
        String dbName = "sale_" + numbers + ".db"; //append number of record at the end of the file

        //if fileName has /transformed/, remove it:
        if (fileName.toLowerCase().contains("/"+TRANSFORM+"/"))
            fileName = fileName.replaceAll("/"+TRANSFORM+"/","");

        logger.log("input fileName: " + fileName + ", dbname: " + dbName);

        try
        {
            //setup S3 client :
            s3client = AmazonS3ClientBuilder.standard().withRegion(AWS_REGION).build();

            //static path to csv file under /tmp to be loaded to db table:
            String csvFilePath = LAMBDA_TEMP_DIRECTORY + fileName;

            // get file from s3, download to /tmp
            File tmpDir = new File(csvFilePath);
            boolean exists = tmpDir.exists();
            if (!exists) {
                getDataFromS3(bucketName, fileName);

            }

            //create table, insert data from csv:
            createDB(csvFilePath, dbName);

            //upload db to s3
            putFileToS3(bucketName, new File(LAMBDA_TEMP_DIRECTORY + dbName));

            //invoke Lambda if required:
            if (invokedLambdaName != null) {
                //invoke next lambda:
                invokeLambda(bucketName,dbName,tableName, transactionId);
            }

            //send response
            setResponseObj(r, true, transactionId, null, bucketName, dbName, tableName);

        }
        catch (Exception e) {
            logger.log("Exception: " + e.toString());
            setResponseObj(r, false, transactionId, e.toString(), null, null, null);
        }

        return r;
    }


    /**
     * create a db if not exists, and insert csv data into table:
     * @param csvFilePath local path to csv file
     * @throws SQLException
     */
    private void createDB(String csvFilePath, String dbName) throws SQLException {
        // Connection string for a file-based SQlite DB
        Connection con = DriverManager.getConnection("jdbc:sqlite:" + dbName);

        // Detect if the table  exists in the database
        PreparedStatement ps = con.prepareStatement("SELECT * FROM sqlite_master WHERE type='table' AND name='" + tableName + "'");
        ResultSet rs = ps.executeQuery();

        // if table doesnt exist, create new one:
        if (!rs.next()) {
            // table does not exist, and should be created
            logger.log(" Table doesnt exist. Creating table: '" + tableName + "'");

            ps = con.prepareStatement("CREATE TABLE " + tableName
                    + "( \"Region\" TEXT,\"Country\" TEXT,\"Item Type\" TEXT,\"Sales Channel\" TEXT," +
                    "\"Order Priority\" TEXT,\"Order Date\" TEXT,\"Order ID\" TEXT PRIMARY KEY, " +
                    "\"Ship Date\" TEXT,\"Units Sold\" TEXT,\"Unit Price\" TEXT,\"Unit Cost\" TEXT," +
                    "\"Total Revenue\" TEXT,\"Total Cost\" TEXT,\"Total Profit\" TEXT," +
                    "\"Order Processing Time\" TEXT, \"Gross Margin\" TEXT);");
            ps.execute();
            rs.close();

            //insert data from csv file to table:
            insertTable(csvFilePath, con);
        } else {
            logger.log("Database \'" + dbName + "' exists with table: " + tableName
                    + ", no new table needs to be created, upload cached table to s3.");
        }
        rs.close();
        con.close();
        logger.log("closing all connection !!!");

    }

    // set response obj
    private void setResponseObj(Response r, boolean success, String transId, String e, String bucketName,
                                       String dbName, String tableName) {
        // Set response object:
        r.setTransactionid(transId);

        if (success) {
            r.setSuccess(true);
            r.setBucketname(bucketName);
            r.setDbname(dbName);
            r.setTablename(tableName);
        }
        else {
            r.setSuccess(false);
            r.setError(e);
        }

    }

    //insert data from csv to table
    private static void insertTable(String csvFilePath, Connection con) {
        System.out.println("insertTable from = " + csvFilePath + ", tablename = "+ tableName);
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
                    String cell = country[i];

                    values += "'" + cell.replaceAll("'", "''") + "'";

                    //add comma after every value except the last value
                    if (i != country.length - 1)
                        values = values + CSV_DELIM;
                } //for

                //insert each row to table:
                PreparedStatement ps = con.prepareStatement("insert into " + tableName + " values(" + values + ");");
                ps.execute();

                if (lineCount%5000 == 0)
                    System.out.println("===> Records inserted: " + lineCount);

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
     * @param objectKey object exported_filename aka name of file in s3, or path of file in s3
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


    /**
     * Invoke another lambda (https://medium.com/@joshua.a.kahn/invoking-an-aws-lambda-function-from-java-29efe3a03fe8)
     * How to build json (https://stackoverflow.com/questions/8876089/how-to-fluently-build-json-in-java)
     * @param bucketname
     * @param dbname
     * @param tablename
     * @return
     * @throws JSONException
     */
    private void invokeLambda(String bucketname, String dbname, String tablename, String tid) throws JSONException {
        if (invokedLambdaName == null || invokedLambdaName.isEmpty()){
            throw new IllegalArgumentException("Environment variables:\"EXTRACTION_LAMBDA_NAME\" is not set");
        }

        //setup lambda invoke client:
        lambdaClient = AWSLambdaClientBuilder.standard().withRegion(AWS_REGION).build();

        //build payload:
        String payload = new JSONObject()
                .put("transactionid", tid)
                .put("bucketname", bucketname)
                .put("dbname", dbname)
                .put("tablename", tablename).toString();

        //invoke another func
        InvokeRequest req = new InvokeRequest()
                .withFunctionName(invokedLambdaName)
                .withPayload(payload);

        // Invoke the function and capture response
        InvokeResult result = lambdaClient.invoke(req);

        if (result.getStatusCode() != HttpStatus.SC_OK) {
            throw new IllegalArgumentException("Failed to invoke lambda " + invokedLambdaName + ". " + result.getFunctionError());
        }
    }

    //just do some simple validation for required fields:
    private String validateParams(Request request) {

        if (request.getTransactionid() == null || request.getTransactionid().isEmpty()) {
            return "\"transactionid\" is required in request";
        }
//
//        if (request.getExported_filename() == null || request.getExported_filename().isEmpty()){
//             return "\"exported_filename\" are required in request";
//        }

        if (bucketName == null || bucketName.isEmpty()){
            return "Environment variables:\"BUCKET_NAME\" is not set";
        }

        if (tableName == null || tableName.isEmpty()){
            return "Environment variables:\"TABLE_NAME\" is not set";
        }

        return null;
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
//        String exported_filename = (args.length > 0 ? args[0] : "");
//
//        // Load the name into the request object
//        req.setBucketname(exported_filename);
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
