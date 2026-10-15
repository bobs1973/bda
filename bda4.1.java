import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper;
 import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashSet; 
import java.util.Set; 
public class MovieTags {

public static class TagMapper extends Mapper<Object, Text, Text, Text> {
private Text movieId = new Text(); private Text tag = new Text();
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
// The line format is: userId, movieId, tag, timestamp String[] tokens = value.toString().split(",");
if (tokens.length == 4) { movieId.set(tokens[1]);
 tag.set(tokens[2]); 
context.write(movieId, tag);
}
}
}

public static class TagReducer extends Reducer<Text, Text, Text, Text> {
private Text result = new Text();
public
void reduce(Text key, Iterable<Text> values, Context context)
throws IOException, InterruptedException { Set<String> tags = new
HashSet<>();
// Collect tags for each movie for (Text val : values) { tags.add(val.toString());
}
// Combine tags into a single string and write output
result.set(String.join(", ", tags)); context.write(key, result);
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

public static void main(String[] args) throws Exception { if (args.length != 2) {

System.err.println("Usage: MovieTags <input path> <output path>");
System.exit(-1);
}
// Configuration and Job setup Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "Movie Tags");
// Set the Jar file job.setJarByClass(MovieTags.class);
// Set Mapper and Reducer classes job.setMapperClass(TagMapper.class); job.setReducerClass(TagReducer.class);
// Set output key and value types job.setOutputKeyClass(Text.class); job.setOutputValueClass(Text.class);
//
Set input and output paths FileInputFormat.addInputPath(job, new
Path(args[0])); FileOutputFormat.setOutputPath(job, new Path(args[1]));
// Wait for job completion System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}

