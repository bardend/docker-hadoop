import java.io.IOException;
import java.io.BufferedReader;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;

public class RegresionLinealCluster {

    // JOB 1 - Calcular sumatorias
    public static class SumMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line.startsWith("FECHA")) return;

            String[] parts = line.split(",");
            if (parts.length > 8) {
                try {
                    double x = Double.parseDouble(parts[7]);
                    double y = Double.parseDouble(parts[8]);
                    context.write(new Text("sum"), new Text(x + "," + y));
                } catch (Exception e) { 
                    // Log the error but don't stop processing
                    System.err.println("Error parsing line: " + line);
                }
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

    // JOB 2 - Calcular coeficientes a, b
    public static class CoefMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("sumatorias")) {
                String[] parts = line.split("\t");
                if (parts.length > 1) {
                    context.write(new Text("coef"), new Text(parts[1]));
                }
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

                double denominator = (n * sumX2 - sumX * sumX);
                if (denominator == 0) {
                    context.write(new Text("error"), new Text("Denominador cero en cálculo de b"));
                    return;
                }

                double b = (n * sumXY - sumX * sumY) / denominator;
                double a = (sumY - b * sumX) / n;

                context.write(new Text("a"), new Text(String.valueOf(a)));
                context.write(new Text("b"), new Text(String.valueOf(b)));
                context.write(new Text("coeficientes"), new Text(a + "," + b));
            }
        }
    }

    // JOB 3 - Calcular predicciones
    public static class PredMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        private double a = 0;
        private double b = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            a = conf.getDouble("a", 0);
            b = conf.getDouble("b", 0);
            System.out.println("Mapper configurado con a=" + a + ", b=" + b);
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (line.startsWith("FECHA")) return;
            
            String[] parts = line.split(",");
            if (parts.length > 8) {
                try {
                    double x = Double.parseDouble(parts[7]);
                    double y = Double.parseDouble(parts[8]);
                    double yPred = a + b * x;
                    context.write(new DoubleWritable(x), new Text(y + "," + yPred));
                } catch (Exception e) { 
                    System.err.println("Error en predicción para línea: " + line);
                }
            }
        }
    }

    public static class PredReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    // Método auxiliar para leer coeficientes
    private static double[] leerCoeficientes(Configuration conf, String outputPath) 
            throws IOException {
        double[] coeficientes = new double[2];
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(outputPath + "/part-r-00000");
        
        if (!fs.exists(path)) {
            throw new IOException("No se encontró el archivo de coeficientes: " + path);
        }

        try (FSDataInputStream in = fs.open(path);
             BufferedReader br = new BufferedReader(new java.io.InputStreamReader(in))) {
            
            String line;
            while ((line = br.readLine()) != null) {
                String[] parts = line.split("\t");
                if (parts.length >= 2) {
                    if ("a".equals(parts[0])) {
                        coeficientes[0] = Double.parseDouble(parts[1]);
                    } else if ("b".equals(parts[0])) {
                        coeficientes[1] = Double.parseDouble(parts[1]);
                    } else if ("coeficientes".equals(parts[0])) {
                        String[] coefs = parts[1].split(",");
                        coeficientes[0] = Double.parseDouble(coefs[0]);
                        coeficientes[1] = Double.parseDouble(coefs[1]);
                        break; // Preferir esta línea si existe
                    }
                }
            }
        }
        
        System.out.println("Coeficientes leídos: a=" + coeficientes[0] + ", b=" + coeficientes[1]);
        return coeficientes;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: RegresionLinealCluster <input-path> <output-path>");
            System.exit(1);
        }

        // Configuración base
        Configuration conf = new Configuration();
        
        // JOB 1 - SUMATORIAS
        Job job1 = Job.getInstance(conf, "regresion-sumatorias");
        job1.setJarByClass(RegresionLinealCluster.class);
        job1.setMapperClass(SumMapper.class);
        job1.setReducerClass(SumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("/temp/sumatorias"));
        
        if (!job1.waitForCompletion(true)) {
            System.err.println("Job 1 falló");
            System.exit(1);
        }

        // JOB 2 - COEFICIENTES
        Job job2 = Job.getInstance(conf, "regresion-coeficientes");
        job2.setJarByClass(RegresionLinealCluster.class);
        job2.setMapperClass(CoefMapper.class);
        job2.setReducerClass(CoefReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("/temp/sumatorias"));
        FileOutputFormat.setOutputPath(job2, new Path("/temp/coeficientes"));
        
        if (!job2.waitForCompletion(true)) {
            System.err.println("Job 2 falló");
            System.exit(1);
        }

        // Leer coeficientes con retry
        double[] coeficientes = null;
        int intentos = 3;
        for (int i = 0; i < intentos; i++) {
            try {
                Thread.sleep(2000); // Esperar 2 segundos para asegurar que los archivos estén escritos
                coeficientes = leerCoeficientes(conf, "/temp/coeficientes");
                break;
            } catch (Exception e) {
                System.err.println("Intento " + (i+1) + " falló: " + e.getMessage());
                if (i == intentos - 1) throw e;
            }
        }

        // JOB 3 - PREDICCIONES
        Configuration conf3 = new Configuration();
        conf3.setDouble("a", coeficientes[0]);
        conf3.setDouble("b", coeficientes[1]);

        Job job3 = Job.getInstance(conf3, "regresion-predicciones");
        job3.setJarByClass(RegresionLinealCluster.class);
        job3.setMapperClass(PredMapper.class);
        job3.setReducerClass(PredReducer.class);
        job3.setMapOutputKeyClass(DoubleWritable.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(DoubleWritable.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));

        boolean success = job3.waitForCompletion(true);
        
        // Limpiar archivos temporales
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path("/temp"), true);
        } catch (Exception e) {
            System.err.println("Error limpiando archivos temporales: " + e.getMessage());
        }

        System.exit(success ? 0 : 1);
    }
}
