import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

public class DesviacionAnimalesHembras {
    
    public static class DesviacionMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text keyText = new Text("desviacion_animales_hembras");
        private DoubleWritable valueDouble = new DoubleWritable();
        
        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] fields = line.split(",");
            
            // Verificar que la línea tenga suficientes campos
            if (fields.length >= 10) {
                try {
                    // Campo 9: NUMERO_ANIMALES HEMBRAS
                    double animalesHembras = Double.parseDouble(fields[9]);
                    valueDouble.set(animalesHembras);
                    context.write(keyText, valueDouble);
                } catch (NumberFormatException e) {
                    // Ignorar líneas con datos inválidos
                }
            }
        }
    }
    
    public static class DesviacionReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            List<Double> valueList = new ArrayList<Double>();
            double sum = 0.0;
            int count = 0;
            
            // Primera pasada: calcular la media
            for (DoubleWritable value : values) {
                double val = value.get();
                valueList.add(val);
                sum += val;
                count++;
            }
            
            if (count > 0) {
                double media = sum / count;
                
                // Segunda pasada: calcular la varianza
                double sumaCuadrados = 0.0;
                for (double val : valueList) {
                    sumaCuadrados += Math.pow(val - media, 2);
                }
                
                double varianza = sumaCuadrados / count;
                double desviacion = Math.sqrt(varianza);
                
                result.set(desviacion);
                context.write(key, result);
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "desviacion animales hembras");
        
        job.setJarByClass(DesviacionAnimalesHembras.class);
        job.setMapperClass(DesviacionMapper.class);
        job.setReducerClass(DesviacionReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
