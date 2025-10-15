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

public class MediaKgCarneHembras {
    
    public static class MediaMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text keyText = new Text("media_kg_carne_hembras");
        private DoubleWritable valueDouble = new DoubleWritable();
        
        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] fields = line.split(",");
            
            // Verificar que la línea tenga suficientes campos
            if (fields.length >= 11) {
                try {
                    // Campo 10: CANTIDAD_KILOGRAMOS_CARNE_HEMBRAS
                    double kgCarneHembras = Double.parseDouble(fields[10]);
                    valueDouble.set(kgCarneHembras);
                    context.write(keyText, valueDouble);
                } catch (NumberFormatException e) {
                    // Ignorar líneas con datos inválidos
                }
            }
        }
    }
    
    public static class MediaReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
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
                double media = sum / count;
                result.set(media);
                context.write(key, result);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "media kg carne hembras");
        
        job.setJarByClass(MediaKgCarneHembras.class);
        job.setMapperClass(MediaMapper.class);
        job.setReducerClass(MediaReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
