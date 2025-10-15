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

public class ConsultaOvinosSechura {
    
    // PRIMER MAPREDUCE: Filtrar por ESPECIE_GANADERA = OVINOS
    public static class FiltroOvinosMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] fields = line.split(",");
            
            // Verificar que la línea tenga suficientes campos
            if (fields.length >= 7) {
                // Campo 6: ESPECIE_GANADERA
                if ("OVINOS".equals(fields[6])) {
                    context.write(new Text("ovinos"), value);
                }
            }
        }
    }
    
    public static class FiltroOvinosReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
    
    // SEGUNDO MAPREDUCE: Filtrar por DISTRITO = SECHURA
    public static class FiltroSechuraMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] fields = line.split(",");
            
            // Verificar que la línea tenga suficientes campos
            if (fields.length >= 6) {
                // Campo 5: DISTRITO
                if ("SECHURA".equals(fields[5])) {
                    context.write(new Text("sechura"), value);
                }
            }
        }
    }
    
    public static class FiltroSechuraReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }
    
    // TERCER MAPREDUCE: Sumar CANTIDAD_KILOGRAMOS_CARNE_HEMBRAS
    public static class SumaKgCarneHembrasMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text keyText = new Text("total_kg_carne_hembras_ovinos_sechura");
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
    
    public static class SumaKgCarneHembrasReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
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
        
        // PRIMER JOB: Filtrar por OVINOS
        Job job1 = Job.getInstance(conf, "filtro ovinos");
        job1.setJarByClass(ConsultaOvinosSechura.class);
        job1.setMapperClass(FiltroOvinosMapper.class);
        job1.setReducerClass(FiltroOvinosReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("/temp/ovinos"));
        
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }
        
        // SEGUNDO JOB: Filtrar por SECHURA
        Job job2 = Job.getInstance(conf, "filtro sechura");
        job2.setJarByClass(ConsultaOvinosSechura.class);
        job2.setMapperClass(FiltroSechuraMapper.class);
        job2.setReducerClass(FiltroSechuraReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, new Path("/temp/ovinos"));
        FileOutputFormat.setOutputPath(job2, new Path("/temp/sechura"));
        
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }
        
        // TERCER JOB: Sumar kg carne hembras
        Job job3 = Job.getInstance(conf, "suma kg carne hembras");
        job3.setJarByClass(ConsultaOvinosSechura.class);
        job3.setMapperClass(SumaKgCarneHembrasMapper.class);
        job3.setReducerClass(SumaKgCarneHembrasReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job3, new Path("/temp/sechura"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
