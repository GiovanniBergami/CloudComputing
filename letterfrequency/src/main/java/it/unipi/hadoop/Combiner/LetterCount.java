package it.unipi.hadoop.Combiner;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class LetterCount {

    // MAPPER
    public static class LetterCountMapper extends Mapper<Object, Text, Text, LongWritable>{

        private final static LongWritable one = new LongWritable(1);

        private Text tot_letters = new Text("total_letters");

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String text = value.toString().toLowerCase();
            for(char c : text.toCharArray()) {

                if (Character.isLetter(c)) {
                    context.write(tot_letters, one);
                }
            }
        }
    }

    //COMBINER corrisponde al reducer perchè è presente una sola chiave
    
    //REDUCER
    public static class LetterCountReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {

            long sum = 0;

            for (LongWritable val : values) {
                sum += val.get();
            }

            result.set(sum);

            context.write(key, result);
        }
    }
}