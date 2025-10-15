import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PromedioAnimalesMachos {
    
    public static class PromedioMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text keyText = new Text("promedio_animales_machos");
        private DoubleWritable valueDouble = new DoubleWritable();
        
        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] fields = line.split(",");
            
            // Verificar que la línea tenga suficientes campos
            if (fields.length >= 8) {
                try {
                    // Campo 7: NUMERO_ANIMALES MACHOS
                    double animalesMachos = Double.parseDouble(fields[7]);
                    valueDouble.set(animalesMachos);
                    context.write(keyText, valueDouble);
                } catch (NumberFormatException e) {
                    // Ignorar líneas con datos inválidos
                }
            }
        }
    }
    
    public static class PromedioReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            double sum = 0.0;
            int count = 0;
            
            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }
            
            if (count > 0) {
                double promedio = sum / count;
                result.set(promedio);
                context.write(key, result);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "promedio animales machos");
        
        job.setJarByClass(PromedioAnimalesMachos.class);
        job.setMapperClass(PromedioMapper.class);
        job.setReducerClass(PromedioReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
