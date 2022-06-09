package org.example;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class CustomLineRecordReader extends RecordReader<LongWritable, Text> {

    private static final Log LOG = LogFactory.getLog(CustomLineRecordReader.class);
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key = null;
    private Text value = null;
    private int lineCount = 1;

    // Metoda are ca si argumente un obiect InputSplit si un obiect TaskAttemptContext
    // si pregateste obiectul de tip RecordReader. Pentru input-urile de tip file-based
    // in cadrul acestei metoade vom idetifica pct de start din cadrul fisierului pt a incepe
    // citirea.
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException {
        lineCount = 1;
        // Acest InputSplit este de tipul FileSplit
        FileSplit split = (FileSplit) genericSplit;
        // Obtinem obiectul Configuration, si setam lungimea maxima pe care o poate avea,
        // in bytes, o linie care va fi citita
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);

        // Split-ul curent "S" este responsabil pentru toate liniile incepand incepand de la
        // pozitia de start si pana la pozitia de end
        start = split.getStart();
        end = start + split.getLength();
        // Obtinem fisierul care a fost split-uit
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(job);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        // Deschidem fisierul si ne pozitionam la inceputul split-ului (blocului)
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        // Daca suntem pe byte-ul 0 atunci prima linie va fi citita, insa daca nu suntem
        // pe byte-ul 0, inseamna ca linia curenta a fost procesata de catre split-ul anterior
        // si trebuie ignorata oarecum
        boolean skipFirstLine = false;
        if (codec != null) {
            in = new LineReader(codec.createInputStream(fileIn), job);
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                // Setam pozitia de start pt citire la "start - 1", acest lucru pentru a fi
                // siguri ca nu sarim peste vreo linie
                // Acesta lucru se poate intampla daca pozitia de start coincide tocmai cu EOL
                --start;
                fileIn.seek(start);
            }
            in = new LineReader(fileIn, job);
        }
        // Daca trebuie sa sarim peste prima linie, citim aceasta linie si o stocam
        // intr-un dummy Text
        if (skipFirstLine) {
            // Resetam pozitia de start ca fiind "start + offset-ul linie"
            start += in.readLine(new Text(), 0,
                    (int) Math.min((long) Integer.MAX_VALUE, end - start));
        }
        this.pos = start;
    }

    // Metoda folosita pentru a putea citi urm pereche (cheie, valoare)
    public boolean nextKeyValue() throws IOException {
        if (key == null) {
            key = new LongWritable();
        }
        // setam cheia, adica numarul linie curente
        key.set(lineCount);
        if (value == null) {
            value = new Text();
        }
        int newSize = 0;
        // Trebuie sa ne asiguram ca avem cel putin o linie de citit in cadrul acestui split
        while (pos < end) {
            lineCount++;
            // citim linia curenta in cadrul var value, astfel obtinem value asociat key-ului
            newSize = in.readLine(value, maxLineLength,
                    Math.max((int) Math.min(Integer.MAX_VALUE, end - pos),
                            maxLineLength));
            // verificam dimensiunea liniei citite
            // Daca este o, insemana ca am ajuns la finalul split-ului si nu mai avem ce sa citim
            // Break si returnam false (no key / value)
            if (newSize == 0) {
                break;
            }
            // Linia a fost citita, resetam pozitia de start (adica this.pos)
            pos += newSize;
            // dimensiunea liniei citita este buna, deci avem perechea (cheie, valoare)
            // break si returnam true
            if (newSize < maxLineLength) {
                break;
            }
            // linia este prea lunga
            LOG.info("Skipped line of size " + newSize + " at pos "
                    + (pos - newSize));
        }
        // Am ajuns la finalul split-ului, deci cheia=null si valoarea=null
        // return false
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            // O noua linie a fost gasita si elementele perechii (cheie, valoare) vor putea
            // fi accesate folosind metodele getCurrentKey getCurrentValue
            return true;
        }
    }

    // Aceasta metoda este folosita de catre framework pentru a furniza cheia din cadrul perechii.
    @Override
    public LongWritable getCurrentKey() {
        return key;
    }

    // Aceasta metoda este folosita de catre framework pentru a furniza valoarea din cadrul perechii.
    @Override
    public Text getCurrentValue() {
        return value;
    }

    // Aceasta metoda este folosita de catre framework pentru reuniunea valorilor
    public float getProgress() {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
    }

    // Metoda folosita de catre framework pentru a face cleanup dupa ce toate perechile
    // (cheie, valoare) au fost procesate
    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }
}