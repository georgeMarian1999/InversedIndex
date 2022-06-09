package org.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class CustomMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();

    // Primeste perechi (key, value), unde key - numarul liniei, value - linia efectiva
    // Mapper-ul va scrie perechi de forma: (cuvant, numeFis-nrLinie)
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        // definim lista de stopWords
        List<String> stopWords = Arrays.asList("and", "or", " ", ".", "padua", "lesin");
        // split-uim linia primita
        StringTokenizer tokenizer = new StringTokenizer(line);
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        // obtinem numele fisierului curent
        String filename = fileSplit.getPath().getName();
        // obtinem fiecare cuv, verif daca se gaseste in stopWards si formam perechi care
        // le trimitem mai departe
        String specialChars = "'*&^%$#@!-+;?><";
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            if (!stopWords.contains(word.toString().toLowerCase().replaceAll(specialChars, ""))) {
                context.write(word, new Text(filename + "-" + key.get()));
            }
        }
    }
}
