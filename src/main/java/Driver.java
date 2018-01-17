import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {
    public static void main (String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String noGram = args[0];
        String threshold = args[1];
        String topK = args[2];
        String input = args[3];
        String nGramLib = args[4];

        Configuration configuration1 = new Configuration();
        Configuration configuration2 = new Configuration();

        configuration2.set("threshold", threshold);
        configuration1.set("noGram", noGram);
        configuration2.set("topK", topK);
        configuration1.set("textinputformat.record.delimiter", ".");


        Job job1 = new Job(configuration1, "NGramLib");  //Job job1 = Job.getInstance(configuration1);
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(NGramLibBuilding.wordSplit.class);
        job1.setCombinerClass(NGramLibBuilding.nGramLibBuilding.class);
        job1.setReducerClass(NGramLibBuilding.nGramLibBuilding.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job1, new Path(input));
        TextOutputFormat.setOutputPath(job1, new Path(nGramLib));
        job1.waitForCompletion(true);

        Job job2 = new Job(configuration2, "LanguageModeling");

        DBConfiguration.configureDB(configuration2,
                "com.mysql.jdbc.Driver",//driver.class
                "jdbc:mysql://192.168.1.101:8889/test",// table name
                "root", // username
                "root"// password
                );

        job2.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.0.8-bin.jar"));

        job2.setJarByClass(Driver.class);
        job2.setMapperClass(LanguageModel.libWordSplit.class);
        job2.setReducerClass(LanguageModel.wordPrediction.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(DBOutputFormat.class);
        DBOutputFormat.setOutput(job2, "output",
                new String[] { "starting_phrases",
                "predicted_word",
                "count"}
                );
        TextInputFormat.addInputPath(job2, new Path(nGramLib));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);



    }
}
