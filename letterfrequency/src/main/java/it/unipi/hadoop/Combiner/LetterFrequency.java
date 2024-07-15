package it.unipi.hadoop.Combiner;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class LetterFrequency {

    // MAPPER
    public static class LetterFrequencyMapper extends Mapper<Object, Text, Text, LongWritable>{

        private final static LongWritable one = new LongWritable(1);

        private Text letter = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String text = value.toString().toLowerCase(); // convert letters in lower case

            for(char c : text.toCharArray()) {

                if (Character.isLetter(c)) {
                    letter.set(StringUtils.stripAccents(Character.toString(c))); // remove accents and diatrical marks
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
            totalLetters = conf.getDouble("totalLetters", 1); // get first job output as totalLetters
        }



        public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {

            double tot = 0;

            for (LongWritable val : values) {
                tot += (double) val.get();
            }

            double result = (double) tot / totalLetters; // frequency calculation
            context.write(key, new DoubleWritable(result));  
        }
    } 
}
