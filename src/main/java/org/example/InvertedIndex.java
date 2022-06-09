package org.example;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex extends Configured implements Tool {

    public int run(String [] args) throws Exception {
        // cream un obiect de tipul Job si ii setam conf
        Job job = Job.getInstance(getConf(), "InvertedIndex");
        //setam clasa asociata job-ului
        job.setJarByClass(InvertedIndex.class);
        //setam numele job-ului
        job.setJobName("invertedindex");

        //setam tipul datelor de iesire, adica tipul pentru key si pentru value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //setam clasele: CustomMapper, CustomCombiner, CustomReducer
        job.setMapperClass(CustomMapper.class);
        job.setCombinerClass(CustomCombiner.class);
        job.setReducerClass(CustomReducer.class);

        //setam tipul datelor de intrare, adica tipul pentru key si pentru value
        job.setInputFormatClass(CustomTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        //setam path-ul pentru citirea datelor, path-ul catre directorul de input
        FileInputFormat.setInputPaths(job, new Path(args[0])); // "hdfs://VMMASTER:9000/input"
        //setam path-ul pentru afisarea datelor, path-ul catre directorul de output
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // "hdfs://VMMASTER:9000/output"

        //obtinem statusul job-ului dupa ce acesta s-a terminat
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new InvertedIndex(), args);
        System.exit(ret);
    }
}

