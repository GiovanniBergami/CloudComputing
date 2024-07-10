package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import it.unipi.hadoop.LetterCount.LetterCountMapper;
import it.unipi.hadoop.LetterCount.LetterCountReducer;
import it.unipi.hadoop.LetterFrequency.LetterFrequencyCombiner;
import it.unipi.hadoop.LetterFrequency.LetterFrequencyMapper;
import it.unipi.hadoop.LetterFrequency.LetterFrequencyReducer;

public class Start {
    public static void main(String[] args) throws Exception {

 Configuration conf = new Configuration();
 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 if (otherArgs.length < 2) {
 System.err.println("Usage: letterfrequency <in> [<in>...] <out>");
 System.exit(2);
 }
 Job job = Job.getInstance(conf, "total letter count");
 job.setJarByClass(LetterCount.class);
 job.setMapperClass(LetterCountMapper.class);
 job.setCombinerClass(LetterCountReducer.class);
 job.setReducerClass(LetterCountReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(LongWritable.class);

 for (int i = 0; i < otherArgs.length - 1; ++i) {
 FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
 }
 Path tempOutputPath = new Path(otherArgs[otherArgs.length - 1] + "_temp");
 FileOutputFormat.setOutputPath(job, tempOutputPath);

 boolean jobSuccess = job.waitForCompletion(true);

     // Secondo job: calcolo delle frequenze delle lettere
     if (jobSuccess) {
        FileSystem fs = FileSystem.get(conf);

        double totalLetters =readLetterCountValue(fs, tempOutputPath);

        System.out.println(totalLetters);
        Job job2 = Job.getInstance(conf, "letter frequency count");
        job2.setJarByClass(LetterFrequency.class);
        job2.setMapperClass(LetterFrequencyMapper.class);
        job2.setCombinerClass(LetterFrequencyCombiner.class);
        job2.setReducerClass(LetterFrequencyReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.getConfiguration().setDouble("totalLetters", totalLetters);

        // Configura gli input e l'output per il secondo job
        //FileInputFormat.setInputPaths(job2, tempOutputPath);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job2, new Path(otherArgs[i]));
            }
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1]));

        // Esegui il secondo job e controlla se è stato completato con successo
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
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
}
