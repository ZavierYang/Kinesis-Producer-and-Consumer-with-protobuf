# Kinesis-Producer-and-Consumer-with-protobuf
## Overview
The repo is a practice of producing and consuming protobuf data to and from Kinesis Data Stream. Beside, after consuming protobuf, it will be converted into parquet file then upload and read parquet file to and from S3 Bucket.

## Introduction
In the Kinesis part, I used Java to implement the Producer and Consumer. There are many Java approaches to implement this part[1, 2]. In my approach, I used Kinesis Library to complete this implementation. Besides, when converting protobuf to parquet and uploading data to S3 bucket, I used Java to finish it as well.

Regarding reading parquet data from S3, I implemented by Python, which will use boto3 to connect with S3 Bucket and concat to a Pandas' dataframe.

In order to be convenient to deploy the AWS resource, you can download the yaml file I wrote (Very simple) and use it on the Clouldformation. Just remember that you need to change the resource name, espacially S3 bucket since it need to be globally unique.

Besides, in Producer and Consumer part, I used AWS workshop[3] and doc[4] as references to finish the implementation. Therefore, if you have any confusion please have a look on those references that I attached bellow.

## Importance
Before starting to play with the code and AWS, remember to set the credential in either credentail file or in your OS environment [5]. There are other ways to set the credential, but I prefere to use these two methods. If your using IntelliJ, you can set credentail file and load into IntelliJ (plugin the AWS tool first) so that your IntelliJ can interact with AWS. This is quite convenient to check whether your credentail is correct or not.

## Data flow


## How to use

## Reference
1. https://docs.aws.amazon.com/streams/latest/dev/building-producers.html
2. https://docs.aws.amazon.com/streams/latest/dev/building-consumers.html
3. https://catalog.us-east-1.prod.workshops.aws/workshops/2300137e-f2ac-4eb9-a4ac-3d25026b235f/en-US
4. https://aws.amazon.com/tw/blogs/big-data/introducing-protocol-buffers-protobuf-schema-support-in-amazon-glue-schema-registry/
5. https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html
6. 
