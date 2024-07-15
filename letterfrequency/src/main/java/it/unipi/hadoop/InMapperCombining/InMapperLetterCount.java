package it.unipi.hadoop.InMapperCombining;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class InMapperLetterCount {

    // IN-MAPPER COMBINER
    public static class LetterCountMapper extends Mapper<Object, Text, Text, LongWritable> {

        private LongWritable count;
        private Text tot_letters = new Text("total_letters");

        public void setup(Context context) {
            count = new LongWritable(0); //initialize count at 0
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String text = value.toString().toLowerCase(); // convert letters in lower case

            for (char c : text.toCharArray()) {

                if (Character.isLetter(c)) {  //consider only characters that are letters

                    count.set(count.get() + 1);
                }
            }

        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(tot_letters, count); 
        }
    }

    // REDUCER
    public static class LetterCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long sum = 0;

            for (LongWritable val : values) {
                sum += val.get();
            }

            result.set(sum);

            context.write(key, result);
        }
    }
}