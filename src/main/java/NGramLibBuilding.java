import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;


public class NGramLibBuilding {
    public static class wordSplit extends Mapper<LongWritable, Text, Text, IntWritable> {
        int noGram;
        public void setup (Context context) {
            Configuration configuration = context.getConfiguration();
            configuration.getInt("noGram", 5);
        }
        public void map (LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim().toLowerCase().replaceAll("[^a-z]+"," ");  //replaceAll
            String[] words = line.split("\\s+");
            if (words.length < 2) {
                return;
            }
            for (int i = 0 ; i < words.length; i++) {    //i < words.length - 1
                StringBuilder sb = new StringBuilder();
                for (int j = 0; i + j < words.length && j < noGram; j++) {
                    sb.append(words[i + j]).append(" ");
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));
                }
            }
        }
    }
    public class nGramLibBuilding extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce (Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value: values) {
                sum += value.get();
            }
            context.write(new Text(key), new IntWritable(sum));
        }
    }
}
