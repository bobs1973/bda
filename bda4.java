import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class MovieTags {
    public static class MovieTagMapper
            extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            if (data.length >= 4) {
                String movieId = data[1];
                String tag = data[2];
                context.write(new Text(movieId), new Text(tag));
            }
        }
    }




    public static class MovieTagReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                            Context context)
                throws IOException, InterruptedException {
            Set<String> uniqueTags = new HashSet<>();
            for (Text value : values) {
                uniqueTags.add(value.toString());
            }
            context.write(
                key,
                new Text(String.join(", ", uniqueTags))
            );
        }
    }

gedit MovieTags.java
export CLASSPATH="$(hadoop classpath)"
javac -source 8 -target 8 MovieTags.java
mkdir MT
cp MovieTags*.class MT/
jar -cvf MT.jar -C MT .
gedit movietags.txt
hdfs dfs -put movietags.txt /input4
hadoop jar MT.jar MovieTags /input4 /output4
hdfs dfs -ls /output4
hdfs dfs -cat /output4/part-r-00000


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MovieLens Movie Tags");
        job.setJarByClass(MovieTags.class);
        job.setMapperClass(MovieTagMapper.class);
        job.setReducerClass(MovieTagReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
