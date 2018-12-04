## Load lambda

Compile the project into a jar file under ```target```.

Then upload to lambda.

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


