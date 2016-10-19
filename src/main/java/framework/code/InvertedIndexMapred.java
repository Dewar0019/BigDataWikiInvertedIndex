package framework.code;


import framework.util.StringIntegerList;
import framework.util.StringIntegerList.StringInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.List;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class InvertedIndexMapred {
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        private Text outputStringInteger = new Text();


        @Override
        public void map(LongWritable offset, Text articleId, Context context) throws IOException, InterruptedException {
            StringIntegerList indices = new StringIntegerList();
            String[] splitItem = articleId.toString().split("@");
            String title = splitItem[0];
            indices.readFromString(splitItem[1].trim());
            List<StringInteger> lemmaFreq = indices.getIndices();
            for (StringInteger pair : lemmaFreq) {
                word.set(pair.getString());
                StringInteger output = new StringInteger(title, pair.getValue());
                String finalOutput = String.format("<%s>", output.toString());
                outputStringInteger.set(finalOutput);
                System.out.println(finalOutput);
                context.write(word, outputStringInteger);
            }
        }
    }





    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text finalList = new Text();

        @Override
        public void reduce(Text lemma, Iterable<Text> articlesAndFreqs, Context context)
                throws IOException, InterruptedException {
            StringIntegerList list = new StringIntegerList();
            for (Text pair : articlesAndFreqs) {
                list.readFromString(pair.toString());
            }
            finalList.set(list.toString());
            context.write(lemma, finalList);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Starting Program InvertedIndexMapred Program");

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: framework.code.InvertedIndexMapred <in> <out>");
            System.exit(2);
        }

        Path inputPath = new Path(otherArgs[0]);
        Path outputPath = new Path(otherArgs[1]);

        Job job = Job.getInstance(conf, "InvertedIndexMapred");

        job.setJarByClass(InvertedIndexMapred.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}