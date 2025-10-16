import java.io.*;
import java.nio.file.FileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RegresionLinealMapReduce {

    // JOB 1: Calcula las sumatorias
    public static class SumMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("FECHA")) return;

            String[] parts = line.split(",");
            try {
                double x = Double.parseDouble(parts[7]);
                double y = Double.parseDouble(parts[8]);
                context.write(new Text("sum"), new Text(x + "," + y));
            } catch (Exception e) {
            }
        }
    }

    public static class SumReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
            int n = 0;

            for (Text val : values) {
                String[] xy = val.toString().split(",");
                double x = Double.parseDouble(xy[0]);
                double y = Double.parseDouble(xy[1]);
                sumX += x;
                sumY += y;
                sumXY += x * y;
                sumX2 += x * x;
                n++;
            }

            context.write(new Text("sumatorias"),
                    new Text(sumX + "," + sumY + "," + sumXY + "," + sumX2 + "," + n));
        }
    }

    // JOB 2: Calcula los coeficientes a y b
    public static class CoefMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value.toString().startsWith("sumatorias")) {
                context.write(new Text("coef"), new Text(value.toString().split("\t")[1]));
            }
        }
    }

    public static class CoefReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                String[] s = val.toString().split(",");
                double sumX = Double.parseDouble(s[0]);
                double sumY = Double.parseDouble(s[1]);
                double sumXY = Double.parseDouble(s[2]);
                double sumX2 = Double.parseDouble(s[3]);
                double n = Double.parseDouble(s[4]);

                double b = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
                double a = (sumY - b * sumX) / n;

                context.write(new Text("a,b"), new Text(a + "," + b));
            }
        }
    }

    // JOB 3: Realiza las predicciones para (x, y_real, y_predicha)
    public static class PredMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        private double a, b;
        protected void setup(Context context) {
            a = context.getConfiguration().getDouble("a", 0);
            b = context.getConfiguration().getDouble("b", 0);
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("FECHA")) return;

            String[] parts = line.split(",");
            try {
                double x = Double.parseDouble(parts[7]);
                double y = Double.parseDouble(parts[8]);
                double yPred = a + b * x;
                context.write(new DoubleWritable(x), new Text(y + "," + yPred));
            } catch (Exception e) {
            }
        }
    }

    public static class PredReducer extends Reducer<DoubleWritable, Text, Text, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                String[] s = val.toString().split(",");
                double yReal = Double.parseDouble(s[0]);
                double yPred = Double.parseDouble(s[1]);
                context.write(new Text(String.valueOf(key.get())),
                        new Text(yReal + "," + yPred));
            }
        }
    }

    // MAIN
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // JOB 1
        Job job1 = Job.getInstance(conf, "regresion-sumatorias");
        job1.setJarByClass(RegresionLinealMapReduce.class);
        job1.setMapperClass(SumMapper.class);
        job1.setReducerClass(SumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // JOB 2
        Job job2 = Job.getInstance(conf, "regresion-coeficientes");
        job2.setJarByClass(RegresionLinealMapReduce.class);
        job2.setMapperClass(CoefMapper.class);
        job2.setReducerClass(CoefReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);

        // Lee los coeficientes desde output2
        Path coefPath = new Path(args[2] + "/part-r-00000");
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(coefPath);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = br.readLine();
        br.close();

        double a = 0, b = 0;
        if (line != null) {
            String[] parts = line.split("\t")[1].split(",");
            a = Double.parseDouble(parts[0]);
            b = Double.parseDouble(parts[1]);
        }

        // JOB 3
        Configuration conf3 = new Configuration();
        conf3.setDouble("a", a);
        conf3.setDouble("b", b);

        Job job3 = Job.getInstance(conf3, "regresion-predicciones");
        job3.setJarByClass(RegresionLinealMapReduce.class);
        job3.setMapperClass(PredMapper.class);
        job3.setReducerClass(PredReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        job3.waitForCompletion(true);

        System.out.println("Coeficientes finales:");
        System.out.println("a = " + a);
        System.out.println("b = " + b);
        System.out.println("Predicciones generadas en carpeta: " + args[3]);
    }
}
