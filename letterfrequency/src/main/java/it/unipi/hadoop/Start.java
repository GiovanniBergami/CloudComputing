package it.unipi.hadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
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

import it.unipi.hadoop.Combiner.LetterCount;
import it.unipi.hadoop.Combiner.LetterCount.LetterCountMapper;
import it.unipi.hadoop.Combiner.LetterCount.LetterCountReducer;
import it.unipi.hadoop.Combiner.LetterFrequency;
import it.unipi.hadoop.Combiner.LetterFrequency.LetterFrequencyCombiner;
import it.unipi.hadoop.Combiner.LetterFrequency.LetterFrequencyMapper;
import it.unipi.hadoop.Combiner.LetterFrequency.LetterFrequencyReducer;
import it.unipi.hadoop.InMapperCombining.InMapperLetterCount;
import it.unipi.hadoop.InMapperCombining.InMapperLetterFrequency;


public class Start {

    private static Integer nReducers = 1; //default value for number of reducers
    private static Boolean inMapper = false; //default combining strategy

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println(
                    "Usage: letterfrequency <in> [<in>...] <out> <nReducers(default 1)> <method(1 for InMapper-Combining)>");
            System.exit(2);
        }

        // otherArgs[2] is nReducers
        if (args.length > 2) {  //if there is a custom value for nReducers or combiner mode

            if (Integer.parseInt(args[2]) != -1) { // -1 can be used to keep the deafult nReducer and specify combiner mode
                nReducers = Integer.parseInt(args[2]);
            }
            
            if (nReducers<-1) {
                System.err.println("Wrong value for number of reducers");
                System.exit(1);
            }
        }

        // otherArgs[3] is combiner mode
        if (args.length > 3) {
            if (Integer.parseInt(args[3]) == 1) {
                inMapper = true;
            }

            if (!(Integer.parseInt(args[3])!=0 || Integer.parseInt(args[3])!=1)) {
                System.err.println("Wrong value for Combiner mode, insert 0 or 1 ");
                System.exit(1);
            }
        }

        Job job = Job.getInstance(conf, "total letter count"); // first map-reduce job to count all the letters in the document

        if (inMapper) { 
            job.setJarByClass(InMapperLetterCount.class);
            job.setMapperClass(InMapperLetterCount.LetterCountMapper.class); //in-mapper combiner
            job.setReducerClass(InMapperLetterCount.LetterCountReducer.class);
        } else {
            job.setJarByClass(LetterCount.class);
            job.setMapperClass(LetterCountMapper.class);
            job.setCombinerClass(LetterCountReducer.class); // in this case we can use the reducer as combiner
            job.setReducerClass(LetterCountReducer.class);
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // set input path

        Path tempOutputPath = new Path(otherArgs[1] + "_temp");
        FileOutputFormat.setOutputPath(job, tempOutputPath); // set output path for the first job 

        long jobStartTime = System.nanoTime();
        boolean jobSuccess = job.waitForCompletion(true);
        double jobTime = (System.nanoTime() - jobStartTime) / 1000000000.0; //save the time of completion of the job


        
        if (jobSuccess) {

            FileSystem fs = FileSystem.get(conf);

            double totalLetters = readLetterCountValue(fs, tempOutputPath); //read the first job output
            System.out.println("\nTotal letters in the document: "+totalLetters+ '\n');

            Job job2 = Job.getInstance(conf, "letter frequency count"); //job2 is the letter frequency count job
            job2.setNumReduceTasks(nReducers); //set the number of reducers (default 1)

            if (inMapper) {
                job2.setJarByClass(InMapperLetterFrequency.class);
                job2.setMapperClass(InMapperLetterFrequency.LetterFrequencyMapper.class); // in-mapper combiner
                job2.setReducerClass(InMapperLetterFrequency.LetterFrequencyReducer.class);
            } else {
                job2.setJarByClass(LetterFrequency.class);
                job2.setMapperClass(LetterFrequencyMapper.class);
                job2.setCombinerClass(LetterFrequencyCombiner.class);
                job2.setReducerClass(LetterFrequencyReducer.class);
            }

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(LongWritable.class);

            job2.getConfiguration().setDouble("totalLetters", totalLetters); //set the total of letters in the configuration

            FileInputFormat.addInputPath(job2, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));

        
            long job2StartTime = System.nanoTime();
            jobSuccess = job2.waitForCompletion(true);
            double job2Time = (System.nanoTime() - job2StartTime) / 1000000000.0; //save the time of completion of the job

            double total_time = jobTime + job2Time; //save the total time

            
            if (jobSuccess) {
                writeToCSV(otherArgs[0], total_time,jobTime,job2Time, nReducers, inMapper); //write statistics in a csv file
                System.exit(0);
            } else
                System.exit(1); // Second job failed

        } else {
            System.exit(1); // first job failed
        }

    }

    //Method to read the output file of the first job
    public static long readLetterCountValue(FileSystem fs, Path outputPath) throws IOException {

        Path path = new Path(outputPath + "/part-r-00000"); //output path

        try (FSDataInputStream inputStream = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("total_letters")) {
                    
                    String[] splits = line.split("\\s+"); // it's a regex for multiple spaces preceded by the java escape character

                    if (splits.length == 2) {
                        return Long.parseLong(splits[1]);
                    } else {
                        System.err.println("The format of the file is not valid: " + line);
                        return -1;
                    }
                }
            }
            return -1;
        }
    }

    //Method to save statistics in a csv file
    public static void writeToCSV(String filename, double totalTime,double timeJob1, double timeJob2, int nReducers, boolean inMapper) {
        String csvFile = "../../Output/output.csv";
        String csvHeader = "Filename,TotalTime,TimeJob1,TimeJob2,Reducers,InMapper";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile, true))) {

            System.out.println("Checking if CSV file is empty...");

            if (new java.io.File(csvFile).length() == 0) { //Write the header if the file is empty
                System.out.println("CSV file is empty, writing header...");
                writer.write(csvHeader);
                writer.newLine();
            }

            System.out.println("Writing data to CSV...");
            //Write data in the csv
            writer.write(filename + "," + totalTime + "," + timeJob1 + "," + timeJob2 + "," +   nReducers + "," + inMapper);
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Error during the writing: " + e.getMessage());
        }
    }
}
