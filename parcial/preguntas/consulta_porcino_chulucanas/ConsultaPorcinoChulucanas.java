import java.io.IOException;
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

public class ConsultaPorcinoChulucanas {
    
    // PRIMER MAPREDUCE: Filtrar por ESPECIE_GANADERA = PORCINO
    public static class FiltroPorcinoMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] fields = line.split(",");
            
            // Verificar que la línea tenga suficientes campos
            if (fields.length >= 7) {
                // Campo 6: ESPECIE_GANADERA
                if ("PORCINO".equals(fields[6])) {
                    context.write(new Text("porcino"), value);
                }
            }
        }
    }
    
    public static class FiltroPorcinoReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
    
    // SEGUNDO MAPREDUCE: Filtrar por DISTRITO = CHULUCANAS
    public static class FiltroChulucanasMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] fields = line.split(",");
            
            // Verificar que la línea tenga suficientes campos
            if (fields.length >= 6) {
                // Campo 5: DISTRITO
                if ("CHULUCANAS".equals(fields[5])) {
                    context.write(new Text("chulucanas"), value);
                }
            }
        }
    }
    
    public static class FiltroChulucanasReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
    
    // TERCER MAPREDUCE: Sumar NUMERO_ANIMALES MACHOS
    public static class SumaAnimalesMachosMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text keyText = new Text("total_animales_machos_porcino_chulucanas");
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
    
    public static class SumaAnimalesMachosReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            double sum = 0.0;
            
            for (DoubleWritable value : values) {
                sum += value.get();
            }
            
            result.set(sum);
            context.write(key, result);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // PRIMER JOB: Filtrar por PORCINO
        Job job1 = Job.getInstance(conf, "filtro porcino");
        job1.setJarByClass(ConsultaPorcinoChulucanas.class);
        job1.setMapperClass(FiltroPorcinoMapper.class);
        job1.setReducerClass(FiltroPorcinoReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("/temp/porcino"));
        
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
        
        // SEGUNDO JOB: Filtrar por CHULUCANAS
        Job job2 = Job.getInstance(conf, "filtro chulucanas");
        job2.setJarByClass(ConsultaPorcinoChulucanas.class);
        job2.setMapperClass(FiltroChulucanasMapper.class);
        job2.setReducerClass(FiltroChulucanasReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, new Path("/temp/porcino"));
        FileOutputFormat.setOutputPath(job2, new Path("/temp/chulucanas"));
        
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
        
        // TERCER JOB: Sumar animales machos
        Job job3 = Job.getInstance(conf, "suma animales machos");
        job3.setJarByClass(ConsultaPorcinoChulucanas.class);
        job3.setMapperClass(SumaAnimalesMachosMapper.class);
        job3.setReducerClass(SumaAnimalesMachosReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job3, new Path("/temp/chulucanas"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
