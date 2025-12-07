package com.example.batchprocessing;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.jerolba.carpet.CarpetWriter;
import com.jerolba.carpet.io.FileSystemOutputFile;

/*
 * To run main app you need to have a parquet file with sample data at /tmp/sample-data.parquet
 * This class creates that file with sample data.
 */
public class PrepareSampleData {

    public static void main(String[] args) throws IOException {
        record SampleRecord(String firstName, String lastName) {
        }

        List<SampleRecord> sampleData = List.of(
                new SampleRecord("Jill", "Doe"),
                new SampleRecord("Joe", "Doe"),
                new SampleRecord("Justin", "Doe"),
                new SampleRecord("Jane", "Doe"),
                new SampleRecord("John", "Doe"));

        FileSystemOutputFile outputFile = new FileSystemOutputFile(new File("/tmp/sample-data.parquet"));
        try (CarpetWriter<SampleRecord> writer = new CarpetWriter<>(outputFile, SampleRecord.class)) {
            writer.write(sampleData);
        }
    }

}
