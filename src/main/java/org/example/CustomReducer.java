package org.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CustomReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        // luam toate valorile care ne vin pentru un key si le concatenam
        String result = "";
        for (Text val : values) {
            result += val + "#";
        }
        context.write(key, new Text(result));
    }
}