package it.unipi.hadoop;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LetterFrequency {

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

 public static class LetterFrequencyReducer extends Reducer<Text,LongWritable,Text,LongWritable> {

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

 public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 if (otherArgs.length < 2) {
 System.err.println("Usage: wordcount <in> [<in>...] <out>");
 System.exit(2);
 }
 Job job = Job.getInstance(conf, "letter frequency count");
 job.setJarByClass(LetterFrequency.class);
 job.setMapperClass(LetterFrequencyMapper.class);
 job.setCombinerClass(LetterFrequencyReducer.class);
 job.setReducerClass(LetterFrequencyReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(LongWritable.class);

 for (int i = 0; i < otherArgs.length - 1; ++i) {
 FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
 }
 FileOutputFormat.setOutputPath(job,new Path(otherArgs[otherArgs.length - 1]));

 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}
