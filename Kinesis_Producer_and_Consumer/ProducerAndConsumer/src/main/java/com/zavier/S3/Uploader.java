package com.zavier.S3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.common.proto.StudentOuterClass;
import org.apache.hadoop.fs.Path;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Logger;

public class Uploader {
    AmazonS3 s3Client;
    private final static Logger logger = Logger.getLogger(Uploader.class.getName());
    public Uploader(String REGION_NAME){
        this.s3Client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .withRegion(REGION_NAME)
                .build();
    }
    public void uploadObject(String bucketName, Path parquetPath, StudentOuterClass.Student student) throws IOException {
        // Check the bucket exits or not
        if ( !s3Client.doesBucketExist(bucketName)) {
            logger.info( "There is no bucket " + bucketName );
            return;
        }

        // Start to upload data into S3 bucket
        try (InputStream is = new FileInputStream(parquetPath.toUri().getPath())){
            s3Client.putObject(
                    new PutObjectRequest(bucketName, student.getUid() + ".parquet", is, new ObjectMetadata()));
            logger.info( "Success putting data into bucket " + bucketName );
        } catch (AmazonServiceException ase) {
            ase.printStackTrace();
        } catch (AmazonClientException ace) {
            ace.printStackTrace();
        }
    }
}
