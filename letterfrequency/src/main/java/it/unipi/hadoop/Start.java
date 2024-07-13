package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import it.unipi.hadoop.Combiner.LetterCount;
import it.unipi.hadoop.Combiner.LetterCount.LetterCountMapper;
import it.unipi.hadoop.Combiner.LetterCount.LetterCountReducer;
import it.unipi.hadoop.Combiner.LetterFrequency;
import it.unipi.hadoop.Combiner.LetterFrequency.LetterFrequencyCombiner;
import it.unipi.hadoop.Combiner.LetterFrequency.LetterFrequencyMapper;
import it.unipi.hadoop.Combiner.LetterFrequency.LetterFrequencyReducer;
import it.unipi.hadoop.InMapperCombining.InMapperLetterCount;
import it.unipi.hadoop.InMapperCombining.InMapperLetterFrequency;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
// nreducers, inputsplit, e combiner/inmapper

public class Start {

    private static Integer nReducers = 1;
    private static Boolean inMapper = false;

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println(
                    "Usage: letterfrequency <in> [<in>...] <out> <nReducers(default 1)> <method(1 for InMapper-Combining)>");
            System.exit(2);
        }
        // otherArgs[2] è nReducers
        if (args.length > 2) {
            if (Integer.parseInt(args[2]) != -1) { // -1 per lasciare il default value, consente di specificare solo il
                                                   // metodo
                nReducers = Integer.parseInt(args[2]);
            }
        }
        // otherArgs[3] è il metodo
        if (args.length > 3) {
            if (Integer.parseInt(args[3]) == 1) {
                inMapper = true;
            }
        }

        Job job = Job.getInstance(conf, "total letter count");
        // job.setNumReduceTasks(nReducers);

        if (!inMapper) {
            job.setJarByClass(LetterCount.class);
            job.setMapperClass(LetterCountMapper.class);
            job.setCombinerClass(LetterCountReducer.class);
            job.setReducerClass(LetterCountReducer.class);
        } else {
            job.setJarByClass(InMapperLetterCount.class);
            job.setMapperClass(InMapperLetterCount.LetterCountMapper.class);
            job.setReducerClass(InMapperLetterCount.LetterCountReducer.class);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

        Path tempOutputPath = new Path(otherArgs[1] + "_temp");
        FileOutputFormat.setOutputPath(job, tempOutputPath);

        long jobStartTime = System.nanoTime();
        boolean jobSuccess = job.waitForCompletion(true);
        double jobTime = (System.nanoTime() - jobStartTime) / 1000000000.0;


        // Secondo job: calcolo delle frequenze delle lettere
        if (jobSuccess) {

            FileSystem fs = FileSystem.get(conf);

            double totalLetters = readLetterCountValue(fs, tempOutputPath);

            System.out.println(totalLetters);
            Job job2 = Job.getInstance(conf, "letter frequency count");
            job2.setNumReduceTasks(nReducers);

            if (!inMapper) {
                job2.setJarByClass(LetterFrequency.class);
                job2.setMapperClass(LetterFrequencyMapper.class);
                job2.setCombinerClass(LetterFrequencyCombiner.class);
                job2.setReducerClass(LetterFrequencyReducer.class);
            } else {
                job2.setJarByClass(InMapperLetterFrequency.class);
                job2.setMapperClass(InMapperLetterFrequency.LetterFrequencyMapper.class);
                job2.setReducerClass(InMapperLetterFrequency.LetterFrequencyReducer.class);
            }

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(LongWritable.class);

            job2.getConfiguration().setDouble("totalLetters", totalLetters);

            // Configura gli input e l'output per il secondo job
            // FileInputFormat.setInputPaths(job2, tempOutputPath);
            FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));

            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

            // Esegui il secondo job e controlla se è stato completato con successo
            long job2StartTime = System.nanoTime();
            jobSuccess = job2.waitForCompletion(true);
            double job2Time = (System.nanoTime() - job2StartTime) / 1000000000.0;

            double total_time = jobTime + job2Time;

            


            if (jobSuccess) {
                writeToCSV(otherArgs[0], total_time,jobTime,job2Time, nReducers, inMapper);
                System.exit(0);
            } else
                System.exit(1);

        } else {
            System.exit(1); // Esce con codice di errore se il primo job non è stato completato con successo
        }

    }

    public static long readLetterCountValue(FileSystem fs, Path outputPath) throws IOException {
        Path path = new Path(outputPath + "/part-r-00000");
        try (FSDataInputStream inputStream = fs.open(path);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("total_letters")) {
                    // Extract value after "letter_count"
                    String[] parts = line.split("\\s+");
                    if (parts.length == 2) {
                        return Long.parseLong(parts[1]);
                    } else {
                        System.err.println("Invalid format in the output file: " + line);
                        return -1;
                    }
                }
            }
            // If "letter_count" line not found
            System.err.println("'letter_count' value not found in the output file file");
            return -1;
        }
    }

    public static void writeToCSV(String filename, double totalTime,double timeJob1, double timeJob2, int nReducers, boolean inMapper) {
        String csvFile = "../../Output/output.csv";
        String csvHeader = "Filename,TotalTime,TimeJob1,TimeJob2,Reducers,InMapper";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile, true))) {

            System.out.println("Checking if CSV file is empty...");

            // Se il file è vuoto, scrive l'intestazione
            if (new java.io.File(csvFile).length() == 0) {
                System.out.println("CSV file is empty, writing header...");
                writer.write(csvHeader);
                writer.newLine();
            }

            System.out.println("Writing data to CSV...");
            writer.write(filename + "," + totalTime + "," + timeJob1 + "," + timeJob2 + "," +   nReducers + "," + inMapper);
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Errore durante la scrittura nel file CSV: " + e.getMessage());
        }
    }
}
