import java.io.IOException;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultiplication {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            // Skip empty or malformed lines
            if (line.isEmpty()) return;

            String[] tokens = line.split(",");
            if (tokens.length != 4) return;

            String matrixName = tokens[0];
            int row = Integer.parseInt(tokens[1]);
            int col = Integer.parseInt(tokens[2]);
            float val = Float.parseFloat(tokens[3]);

            Text outputKey = new Text();
            Text outputValue = new Text();

            if (matrixName.equals("A")) {
                // A[i][k], emit for all columns j in result matrix (since B is 2x2)
                for (int j = 0; j < 2; j++) {
                    outputKey.set(row + "," + j); // key = i,j
                    outputValue.set("A," + col + "," + val); // A,k,value
                    context.write(outputKey, outputValue);
                }
            } else if (matrixName.equals("B")) {
                // B[k][j], emit for all rows i in result matrix (since A is 2x2)
                for (int i = 0; i < 2; i++) {
                    outputKey.set(i + "," + col); // key = i,j
                    outputValue.set("B," + row + "," + val); // B,k,value
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, FloatWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            List<Entry<Integer, Float>> listA = new ArrayList<>();
            List<Entry<Integer, Float>> listB = new ArrayList<>();

            for (Text val : values) {
                String[] tokens = val.toString().split(",");
                if (tokens.length != 3) continue;

                String matrixName = tokens[0];
                int index = Integer.parseInt(tokens[1]);
                float value = Float.parseFloat(tokens[2]);

                if (matrixName.equals("A")) {
                    listA.add(new SimpleEntry<>(index, value));
                } else if (matrixName.equals("B")) {
                    listB.add(new SimpleEntry<>(index, value));
                }
            }

            float sum = 0;
            for (Entry<Integer, Float> a : listA) {
                for (Entry<Integer, Float> b : listB) {
                    if (a.getKey().equals(b.getKey())) {
                        sum += a.getValue() * b.getValue();
                    }
                }
            }

            context.write(key, new FloatWritable(sum));
        }
    }
gedit MatrixMultiplication.java
export CLASSPATH="$(hadoop classpath)"
javac -source 8 -target 8 MatrixMultiplication.java
mkdir MM
cp MatrixMultiplication*.class MM/
jar -cvf MM.jar -C MM .
gedit matrix.txt
hdfs dfs -put matrix.txt /input2
hadoop jar MM.jar MatrixMultiplication /input2 /output2
hdfs dfs -ls /output2
hdfs dfs -cat /output2/part-r-00000
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MatrixMultiplication2x2");

        job.setJarByClass(MatrixMultiplication.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));     // Input path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));   // Output path

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
