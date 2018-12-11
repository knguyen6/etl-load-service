## Load lambda

Compile the project into a jar file under ```target```.

Then upload to lambda.

## Handler: 
```lambda.ETL_Load::handleRequest```

## Runtime: 
```Java8```

## Memory(MB): 
```512MB``` at least

### Timeout
depend, start with 1 or 2 mins, could go up to 15 mins.


## Environment Variables
Your lambda will need these environment variables (Add environment vars on ur Lambda UI, under the upload for source code section): 


```TABLE_NAME : my_table```

```BUCKET_NAME: my_bucket```



## Request
Request body required ```bucketname``` and ```filename```

Example: 

```
{
  "transactionid" : "some_unique_trans_id",
  "filename" : "my_file.csv"
}
```

## Response:
```
{
  "success": true/false,
  "bucketname":  "my_bucket",
  "dbname" : "my_db.db",
  "tablename" : "my_table",
  "error" : "<error if success=false>",
  "transactionid":"some_unique_trans_id"
}
```

