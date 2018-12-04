#!/bin/bash

# JSON object to pass to Lambda Function
json={"\"name\"":"\"Jill\u0020Jone\",\"param1\"":1,"\"param2\"":2,"\"key\"":"\"hellosqlite\""}

#echo "Invoking Lambda function using API Gateway"
#time output=`curl -s -H "Content-Type: application/json" -X POST -d  $json {INSERT API GATEWAY URL HERE}`
#
#echo ""
#echo "CURL RESULT:"
#echo $output
#echo ""
#echo ""

echo "Invoking Lambda function using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name processcsv --region us-east-1 --payload $json /dev/stdout | head -n 1 | head -c 200000 ; echo`
echo ""
echo "AWS CLI RESULT:"
echo $output
echo ""







