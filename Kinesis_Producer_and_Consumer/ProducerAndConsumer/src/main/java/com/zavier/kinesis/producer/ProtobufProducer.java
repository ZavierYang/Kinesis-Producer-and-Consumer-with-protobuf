package com.zavier.kinesis.producer;

import com.amazonaws.auth.*;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.common.proto.StudentOuterClass;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import software.amazon.awssdk.regions.Region;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.logging.Logger;

public class ProtobufProducer {
    // Remember to change the following constants to your own info.
    private final String REGION_NAME = "us-west-2";
    private final String STREAM_NAME = "ZavierStreamming";
    private final int MAX_GENERATION = 1;
    private final static Logger logger = Logger.getLogger(ProtobufProducer.class.getName());

    public static void main(String[] args) throws Exception {
        ProtobufProducer producer = new ProtobufProducer();
        producer.produceData();
    }

    public void produceData() throws Exception {
        // Config AWS Credentials
        DefaultAWSCredentialsProviderChain provider = new DefaultAWSCredentialsProviderChain();

        // Config kinesis data steam in order to send data in to kinesis
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                .setCredentialsProvider(provider)
                .setRecordMaxBufferedTime(3000)
                .setMaxConnections(1)
                .setRequestTimeout(60000)
                .setRegion(REGION_NAME);
        final KinesisProducer producer = new KinesisProducer(config);

        logger.info("Starting KPL to generate data, and produce to" + STREAM_NAME
                        + " in the " + REGION_NAME + " region.");

        // Mon-callback method
        int count = 0;
        // Used for checking sending result
        List<Future<UserRecordResult>> putFutures = new LinkedList<Future<UserRecordResult>>();
        while (count < MAX_GENERATION){
            StudentOuterClass.Student student = createStudent();
            ByteBuffer data = ByteBuffer.wrap(student.toByteArray());
            putFutures.add(producer.addUserRecord(STREAM_NAME, student.getUid(), data));
            logger.info("Produce a student " + student.toString() + " and send to kinesis.");
            count++;
        }

        // Wait for result
        int success = 0, fail = 0;
        for (Future<UserRecordResult> f : putFutures) {
            UserRecordResult result = f.get();
            if (result.isSuccessful()) success++;
            // You could also use "Attempt" to get the error sending message (please google lol)
            else fail++;
        }
        logger.info(success + " data has been send and " + fail + " fail");

        // Callback method
        // We could use callback to get the sending result as well, but need to have busy waiting to get the result
        // when sending data
//        FutureCallback<UserRecordResult> myCallback = new FutureCallback<UserRecordResult>() {
//            @Override public void onFailure(Throwable t) {
//                /* Analyze and respond to the failure  */
//                t.printStackTrace();
//            };
//            @Override public void onSuccess(UserRecordResult result) {
//            };
//        };
//
//        while (true){
//            if(count < MAX_GENERATION){
//                StudentOuterClass.Student student = createStudent();
//                ByteBuffer data = ByteBuffer.wrap(student.toByteArray());
//                ListenableFuture<UserRecordResult> f = producer.addUserRecord(STREAM_NAME, student.getUid(), data));
//
//                Futures.addCallback(f, myCallback, MoreExecutors.directExecutor());
//                logger.info("Produce a student " + student.toString() + " and send to kinesis.");
//                count++;
//            }
//        }
    }

    public StudentOuterClass.Student createStudent(){
        String uid = UUID.randomUUID().toString();
        StudentOuterClass.Student student = StudentOuterClass.Student.newBuilder()
                .setUid(uid)
                .setName("student " + uid)
                .setAge(new Random().nextInt(100))
                .setGender("unreveal")
                .build();
        return student;
    }
}
