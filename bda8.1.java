import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    
    public static class TokenizerMapper
         extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().replaceAll("[^a-zA-Z0-9]", "").toLowerCase());
                if (!word.toString().isEmpty()) {
                    context.write(word, one);
                }
            }
        }
    }

    
    public static class IntSumReducer
         extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
                           ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

gedit WordCount.java
export CLASSPATH="$(hadoop classpath)"
javac -source 8 -target 8 WordCount.java
mkdir WC
cp WordCount*.class WC/
jar -cvf WC.jar -C WC .
gedit word.txt
hdfs dfs -put word.txt /input8
hadoop jar WC.jar WordCount /input8 /output8
hdfs dfs -ls /output8
hdfs dfs -cat /output8/part-r-00000    

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class); // Optional combiner
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));  // HDFS input
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // HDFS output

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
