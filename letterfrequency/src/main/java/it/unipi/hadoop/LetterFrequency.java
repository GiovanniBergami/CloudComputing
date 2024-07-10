package it.unipi.hadoop;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import it.unipi.hadoop.LetterCount.LetterCountMapper;
import it.unipi.hadoop.LetterCount.LetterCountReducer;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

public class LetterFrequency {

// MAPPER
 public static class LetterFrequencyMapper extends Mapper<Object, Text, Text, LongWritable>{

 private final static LongWritable one = new LongWritable(1);

 private Text letter = new Text();

 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

 String data = value.toString().toLowerCase();
for(char c : data.toCharArray()) {

    if (Character.isLetter(c)) {
        letter.set(Character.toString(c));
        context.write(letter, one);
    }
 }
 }
 }

 //COMBINER
 public static class LetterFrequencyCombiner extends Reducer<Text,LongWritable,Text,LongWritable> {

 private LongWritable result = new LongWritable();

 public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
   
long tot = 0;
   
for (LongWritable val : values) {
    tot += val.get();
 }
   
 result.set(tot);
   
context.write(key, result);
}
}
   

//REDUCER
 public static class LetterFrequencyReducer extends Reducer<Text,LongWritable,Text,DoubleWritable> {

    private double totalLetters;

    
    public void setup(Context context) {
        Configuration conf = context.getConfiguration();
        totalLetters = conf.getDouble("totalLetters", 1); // 1 default value
    }



 public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {

 double tot = 0;

 for (LongWritable val : values) {
 tot += (double) val.get();
 }

double result = (double) tot / totalLetters;
context.write(key, new DoubleWritable(result));
 
 }
 }

 
}
