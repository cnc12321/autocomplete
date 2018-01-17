import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class LanguageModel {
    public static class libWordSplit extends Mapper<LongWritable, Text, Text, Text> {
        int threshold;
        public void setup (Context context) {
            Configuration configuration = context.getConfiguration();
            configuration.getInt("threshold",5);
        }
        public void map (LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value == null || value.toString().trim().length() == 0) {
                return;
            }
            String line = value.toString().trim();
            String[] mix = line.split("\t");
            if (mix.length < 2) {
                return;    //should throw customized exception
            }
            String phrase = mix[0];
            int count = Integer.parseInt(mix[1]);
            if (count < threshold) {
                return;     //throw the word that less than the required no.
            }
            String[] words = phrase.split("\\s+");
            StringBuilder keyBuilder = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                keyBuilder.append(words[i]).append(" ");
            }
            String keyOutput = keyBuilder.toString().trim();
            String lastWord = words[words.length - 2];
            StringBuilder sb = new StringBuilder();
            sb.append(lastWord).append("=").append(count);
            if (keyOutput != null && keyOutput.length() >= 1) {   //防止出现在数据库中间出现起始词为空的情况
                context.write(new Text(keyOutput), new Text(sb.toString().trim()));
            }
        }
    }
    public static class wordPrediction extends Reducer<Text, Text, DBOutputWritable, NullWritable> {
        int topK;
        public void setup (Context context) {
            Configuration configuration = context.getConfiguration();
            configuration.getInt("topK", 10);
        }
        public void reduce (Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            TreeMap<Integer, ArrayList<String>> tm = new TreeMap<Integer, ArrayList<String>>(Collections.reverseOrder());
            for (Text value: values) {
                String[] content = value.toString().trim().split("=");
                String following_words = content[0].trim();
                int count = Integer.parseInt(content[1].trim());
                if (tm.containsKey(count)) {
                    tm.get(count).add(following_words);
                }
                else {
                    ArrayList<String> ls = new ArrayList<String>();
                    ls.add(following_words);
                    tm.put(count, ls);
                }
            }

            Iterator<Integer> iter = tm.keySet().iterator();
            for (int i = 1; iter.hasNext() && i <= topK;) {
                int count = iter.next();
                ArrayList<String> temp = tm.get(count);
                for (String t: temp) {
                    context.write(new DBOutputWritable(key.toString().trim(),
                            t.trim(), count), NullWritable.get());
                    i++;
                }
            }
        }
    }
}
