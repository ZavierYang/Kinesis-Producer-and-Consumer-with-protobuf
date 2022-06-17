package com.zavier.kinesis.consumer;


import com.common.proto.StudentOuterClass;
import com.common.utils.ParquetConverter;
import com.zavier.S3.Uploader;
import org.apache.hadoop.fs.Path;
import software.amazon.awssdk.regions.Region;
import org.apache.commons.lang3.ObjectUtils;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.common.KinesisClientUtil;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class ProtobufConsumer {
    private final static Logger logger = Logger.getLogger(ProtobufConsumer.class.getName());

    // Remember to change the following constants to your own info except APPLICATION_NAME,
    // APPLICATION_NAME can be any name you want to give.
    private static String REGION_NAME = "us-west-2";
    private static String STREAM_NAME = "ZavierStreamming";
    private static String APPLICATION_NAME = "studentConsumer";
    private static String BUCKET_NAME = "zavierstorage";

    private static Uploader uploader;

    public static void main(String[] args){
        ProtobufConsumer protobufConsumer = new ProtobufConsumer();
        protobufConsumer.run();
    }

    public void run(){
        logger.info("Starting KCL to consume data from kinesis");
        // Configure kinesis
        Region region = Region.of(REGION_NAME);

        // You do not need to deploy DynamoDB and CloudWatch
        KinesisAsyncClient kinesisClient = KinesisClientUtil.createKinesisAsyncClient(KinesisAsyncClient.builder().region(region));
        DynamoDbAsyncClient dynamoClient = DynamoDbAsyncClient.builder().region(region).build();
        CloudWatchAsyncClient cloudWatchClient = CloudWatchAsyncClient.builder().region(region).build();

        // Create the factory which process the data from kinesis
        StudentProcessorFactory studentProcessorFactory = new StudentProcessorFactory();
        ConfigsBuilder configsBuilder = new ConfigsBuilder(STREAM_NAME,
                                                            APPLICATION_NAME,
                                                            kinesisClient,
                                                            dynamoClient,
                                                            cloudWatchClient,
                                                            UUID.randomUUID().toString(),
                                                            studentProcessorFactory);

        Scheduler scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                configsBuilder.coordinatorConfig(),
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig(),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig()
        );

        // Create uploader first in order to avoid creating S3client repeatedly
        this.uploader = new Uploader(REGION_NAME);

        // Start to consume data
        Thread schedulerThread = new Thread(scheduler);
        schedulerThread.setDaemon(true);
        schedulerThread.start();

        // Shutdown process
        logger.info("Press enter to shutdown");
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            reader.readLine();
            Future<Boolean> gracefulShutdownFuture = scheduler.startGracefulShutdown();
            logger.info("Waiting up to 20 seconds for shutdown to complete.");
            gracefulShutdownFuture.get(20, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.info("Interrupted while waiting for graceful shutdown. Continuing.");
        }
        logger.info("Completed, shutting down now.");
    }

    private static class StudentProcessorFactory implements ShardRecordProcessorFactory {
        public ShardRecordProcessor shardRecordProcessor() {
            return new StudentRecordProcessor();
        }
    }
    private static class StudentRecordProcessor implements ShardRecordProcessor {

        @Override
        public void initialize(InitializationInput initializationInput) {
            try {
                logger.info("Initializing @ Sequence: " + initializationInput.extendedSequenceNumber());
            } catch (Exception e){
                e.printStackTrace();
            }
        }

        @Override
        public void processRecords(ProcessRecordsInput processRecordsInput) {
            try {
                logger.info("Processing " + processRecordsInput.records().size() + " record(s)");
                for (KinesisClientRecord record : processRecordsInput.records()) {
                    // Deserializing protobuf message into schema generated POJO
                    byte[] array = new byte[record.data().remaining()];
                    record.data().get(array);
                    StudentOuterClass.Student student = StudentOuterClass.Student.parseFrom(array);

                    logger.info("Processed record: " + student);
                    logger.info("Student Id: " + student.getUid() + " | Name: "  + student.getName() +
                            " | Age: " + student.getAge() + " | Gender: " + student.getGender());

                    // Start to convert to parquet
                    java.nio.file.Path tempFilePath = Files.createTempFile(student.getUid(), ".parquet" );
                    Path filepath = new Path(tempFilePath.toUri());
                    ParquetConverter.converToParquet(filepath, student);

                    // Upload to S3
                    uploader.uploadObject(BUCKET_NAME, filepath, student);
                }
            } catch (Exception e) {
                logger.info("Caught throwable while processing records. Aborting.");
                e.printStackTrace();
                Runtime.getRuntime().halt(1);
            }
        }

        @Override
        public void leaseLost(LeaseLostInput leaseLostInput) {
            try {
                logger.info("Lost lease, so terminating.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void shardEnded(ShardEndedInput shardEndedInput) {
            try {
                logger.info("Reached shard end checkpointing.");
                shardEndedInput.checkpointer().checkpoint();
            } catch (ShutdownException | InvalidStateException e) {
                logger.info("Exception while checkpointing at shard end. Giving up.");
                e.printStackTrace();
            }
        }

        @Override
        public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
            try {
                logger.info("Scheduler is shutting down, checkpointing.");
                shutdownRequestedInput.checkpointer().checkpoint();
            } catch (ShutdownException | InvalidStateException e) {
                logger.info("Exception while checkpointing at requested shutdown. Giving up.");
                e.printStackTrace();
            }
        }
    }
}
