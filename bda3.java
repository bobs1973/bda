import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class WeatherMapReduce1 {
    public static class WeatherMapper
            extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            String city = data[0];
            double temperature = Double.parseDouble(data[1]);
            String condition;
            if (temperature < 0) {
                condition = "Freezing";
            } else if (temperature <= 15) {
                condition = "Cold";
            } else if (temperature <= 25) {
                condition = "Warm";
            } else {
                condition = "Hot";
            }
            context.write(new Text(city), new Text(condition));
        }
    }



    public static class WeatherReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                            Context context)
                throws IOException, InterruptedException {
            StringBuilder result = new StringBuilder();
            for (Text value : values) {
                if (result.length() > 0) {
                    result.append(",");
                }
                result.append(value.toString());
            }
            context.write(key, new Text(result.toString()));
        }
    }

gedit WeatherMapReduce1.java
export CLASSPATH="$(hadoop classpath)"
javac -source 8 -target 8 WeatherMapReduce1.java
mkdir WM
cp WeatherMapReduce1*.class WM/
jar -cvf WM.jar -C WM .
gedit weather.txt
hdfs dfs -put weather.txt /input3
hadoop jar WM.jar WeatherMapReduce1 /input3 /output3
hdfs dfs -ls /output3
hdfs dfs -cat /output3/part-r-00000


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weather Data Mining");
        job.setJarByClass(WeatherMapReduce1.class);
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

