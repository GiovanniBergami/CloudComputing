package it.unipi.hadoop.InMapperCombining;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.commons.lang3.StringUtils;

public class InMapperLetterFrequency {

    // IN MAPPER COMBINING
    public static class LetterFrequencyMapper extends Mapper<Object, Text, Text, LongWritable>{

        private final static LongWritable one = new LongWritable(1);
        
        private Map<Text, LongWritable> lettersCounter;


        protected void setup(Context context) {
            lettersCounter = new HashMap<Text, LongWritable>(); 
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String data = value.toString().toLowerCase();
            for(char c : data.toCharArray()) {
                
                if (Character.isLetter(c)) {

                    Text letter = new Text(StringUtils.stripAccents(Character.toString(c))); // rimozioni accenti

                    if (lettersCounter.containsKey(letter)) {

                        lettersCounter.replace(letter,new LongWritable(lettersCounter.get(letter).get() + 1 ) );

                    }
                    else{
                        lettersCounter.put(letter, one);
                    }
                    
                }
            }
        }

       // public void cleanup(Context context) throws IOException, InterruptedException {
         //   for (Text t : lettersCounter.keySet()) {
           //     System.out.println(t.toString());
             //   context.write(t, lettersCounter.get(t));
            //}
        //}

            protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Text, LongWritable> entry : lettersCounter.entrySet()) {
                context.write(entry.getKey(), entry.getValue());
            }
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
