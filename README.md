## Load lambda

Compile the project into a jar file under ```target```.

Then upload to lambda.

## Environment Variables
Your lambda will need these environment variables (Add environment vars on ur Lambda UI, under the upload for source code section): 



```BUCKET_NAME : <whatever bucket you're using>```

```DB_NAME : sale.db```

```OUTPUT_FILE_NAME : outfile_100SalesRecords.csv```

```TABLE_NAME : sale_table```


## Request
Currently it doesn't need a request body.
