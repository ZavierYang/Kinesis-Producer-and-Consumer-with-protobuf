package com.common.utils;

import com.common.proto.StudentOuterClass;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.proto.ProtoParquetWriter;

import java.io.IOException;
import java.util.logging.Logger;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;

public class ParquetConverter {
    private final static Logger logger = Logger.getLogger(ParquetConverter.class.getName());

    public static void converToParquet(Path filepath, StudentOuterClass.Student student) throws IOException {
        logger.info( "start to write parquet file by uid " + student.getUid() );
        try (ParquetWriter writer = ProtoParquetWriter.builder(filepath)
                .withWriteMode(OVERWRITE)
                .withCompressionCodec(SNAPPY)
                .withPageSize( ParquetWriter.DEFAULT_PAGE_SIZE )
                .withMessage(student.getClass())
                .build()) {

            writer.write(student);
        }
    }
}
