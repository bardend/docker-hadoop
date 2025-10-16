import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.*;

public class NaiveBayesGanado {

    // MAPPER 1: Calcular estadísticas por clase (especie ganadera)
    public static class StatisticsMapper extends Mapper<Object, Text, Text, Text> {
        
        @Override
        protected void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            if (line.contains("ESPECIE_GANADERA")) return; // Saltar encabezado
            
            String[] fields = line.split(",");
            if (fields.length < 11) return;
            
            try {
                String especie = fields[6].trim();
                double machos = Double.parseDouble(fields[7].trim());
                double hembras = Double.parseDouble(fields[9].trim());
                double carneMachos = Double.parseDouble(fields[8].trim());
                double carneHembras = Double.parseDouble(fields[10].trim());
                
                // Emitir: clase -> valores para estadísticas
                context.write(
                    new Text(especie),
                    new Text(machos + "," + hembras + "," + carneMachos + "," + carneHembras)
                );
            } catch (NumberFormatException e) {
                // Ignorar líneas mal formadas
            }
        }
    }

    // REDUCER 1: Calcular media, varianza y probabilidades
    public static class StatisticsReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            List<double[]> records = new ArrayList<>();
            int count = 0;
            
            // Recolectar todos los valores para esta clase
            for (Text value : values) {
                String[] vals = value.toString().split(",");
                double[] nums = new double[4];
                for (int i = 0; i < 4; i++) {
                    nums[i] = Double.parseDouble(vals[i]);
                }
                records.add(nums);
                count++;
            }
            
            if (count == 0) return;
            
            // Calcular media y varianza para cada atributo
            double[] means = new double[4];
            double[] variances = new double[4];
            
            // Calcular medias
            for (double[] record : records) {
                for (int i = 0; i < 4; i++) {
                    means[i] += record[i];
                }
            }
            for (int i = 0; i < 4; i++) {
                means[i] /= count;
            }
            
            // Calcular varianzas
            for (double[] record : records) {
                for (int i = 0; i < 4; i++) {
                    double diff = record[i] - means[i];
                    variances[i] += diff * diff;
                }
            }
            for (int i = 0; i < 4; i++) {
                variances[i] /= (count - 1);
                if (variances[i] == 0) variances[i] = 1e-9; // Evitar división por cero
            }
            
            // Calcular probabilidad a priori (probabilidad de la clase)
            double priorProbability = (double) count / 3197; // Total de registros
            
            // Emitir resultado
            StringBuilder sb = new StringBuilder();
            sb.append("count=").append(count).append("|");
            sb.append("prior=").append(priorProbability).append("|");
            
            // Means
            for (int i = 0; i < 4; i++) {
                sb.append("mean").append(i).append("=").append(means[i]).append("|");
            }
            
            // Variances
            for (int i = 0; i < 4; i++) {
                sb.append("var").append(i).append("=").append(variances[i]).append("|");
            }
            
            context.write(key, new Text(sb.toString()));
        }
    }

    // MAPPER 2: Preparar datos para prueba (si se necesita validar)
    public static class PredictionMapper extends Mapper<Object, Text, Text, Text> {
        
        @Override
        protected void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            String line = value.toString();
            if (line.contains("ESPECIE_GANADERA")) return;
            
            String[] fields = line.split(",");
            if (fields.length < 11) return;
            
            try {
                String departamento = fields[3].trim();
                double machos = Double.parseDouble(fields[7].trim());
                double hembras = Double.parseDouble(fields[9].trim());
                double carneMachos = Double.parseDouble(fields[8].trim());
                double carneHembras = Double.parseDouble(fields[10].trim());
                
                String features = machos + "," + hembras + "," + carneMachos + "," + carneHembras;
                context.write(new Text(departamento), new Text(features));
            } catch (NumberFormatException e) {
                // Ignorar
            }
        }
    }

    // REDUCER 2: Aplicar Naive Bayes para clasificación
    public static class PredictionReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {
            
            int correctPredictions = 0;
            int totalPredictions = 0;
            
            for (Text value : values) {
                totalPredictions++;
                // Aquí iría la lógica de predicción con Naive Bayes
            }
            
            String result = String.format("Departamento: %s | Predicciones: %d/%d",
                key.toString(), correctPredictions, totalPredictions);
            context.write(key, new Text(result));
        }
    }

    // Main - Configurar y ejecutar jobs
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: hadoop jar NaiveBayesGanado.jar <inputPath> <outputPath>");
            System.exit(1);
        }

        // JOB 1: Calcular estadísticas
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Naive Bayes Statistics");
        job1.setJarByClass(NaiveBayesGanado.class);
        job1.setMapperClass(StatisticsMapper.class);
        job1.setReducerClass(StatisticsReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/statistics"));
        
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // JOB 2: Predicción/Clasificación
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Naive Bayes Prediction");
        job2.setJarByClass(NaiveBayesGanado.class);
        job2.setMapperClass(PredictionMapper.class);
        job2.setReducerClass(PredictionReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/predictions"));
        
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
