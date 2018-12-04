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

```DB_NAME : my_db.db```

```TABLE_NAME : my_table```


## Request
Request body required ```bucketname``` and ```filename```

Example: 

```
{
  "bucketname" : "my_bucket",
  "filename" : "my_file.csv"
}
```

## Response:
```
{
  "success": true/false,
  "bucketname":  "my_bucket",
  "dbname" : "mydb.db",
  "tablename" : "my_table",
  "error" : "<error if success=false>"
}
```

