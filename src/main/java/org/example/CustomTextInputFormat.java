package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class CustomTextInputFormat extends FileInputFormat<LongWritable, Text> {

    // Suprascriem metoda si folosim obiectul MyLineRecordReader pentru
    // a citi linie cu linie
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit arg0,
                                                               TaskAttemptContext arg1) throws IOException, InterruptedException {
        return new CustomLineRecordReader();
    }

    @Override
    public boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}

