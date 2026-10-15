import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class MatrixMultiplication {
    public static class MatrixMapper
            extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            String matrix = tokens[0];
            int row = Integer.parseInt(tokens[1]);
            int col = Integer.parseInt(tokens[2]);
            double val = Double.parseDouble(tokens[3]);
            if (matrix.equals("A")) {
                for (int j = 0; j < 2; j++) {
                    context.write(
                        new Text(row + "," + j),
                        new Text("A," + col + "," + val)
                    );
                }
            } else {
                for (int i = 0; i < 2; i++) {
                    context.write(
                        new Text(i + "," + col),
                        new Text("B," + row + "," + val)
                    );
                }
            }
        }
    }

    
    public static class MatrixReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> aValues = new ArrayList<>();
            List<String> bValues = new ArrayList<>();
            for (Text value : values) {
                String[] parts = value.toString().split(",");
                if (parts[0].equals("A")) {
                    aValues.add(value.toString());
                } else {
                    bValues.add(value.toString());
                }
            }
            double sum = 0;
            for (String a : aValues) {
                String[] aParts = a.split(",");
                int k = Integer.parseInt(aParts[1]);
                double aVal = Double.parseDouble(aParts[2]);
                for (String b : bValues) {
                    String[] bParts = b.split(",");
                    int bK = Integer.parseInt(bParts[1]);
                    double bVal = Double.parseDouble(bParts[2]);
                    if (k == bK) {
                        sum += aVal * bVal;
                    }
                }
            }
            context.write(key, new Text(String.valueOf(sum)));
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
        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
