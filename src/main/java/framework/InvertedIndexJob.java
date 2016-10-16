package framework;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import framework.code.articles.GetArticlesMapred;
import framework.code.inverted.InvertedIndexMapred;
import framework.code.lemma.LemmaIndexMapred;
import framework.util.StringIntegerList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.IOException;

/**
 * Created by dewartan on 10/15/16.
 */
public class InvertedIndexJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Starting Program Inverted Index Program");

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: invertedIndex <in> <out>");
            System.exit(2);
        }

        Path inputPath = new Path(otherArgs[0]);
        Path outputPath = new Path(otherArgs[1]);

        //Grab the resource files
        ClassLoader cl = InvertedIndexJob.class.getClassLoader();
        String peopleListFile = cl.getResource("people.txt").getFile();
        String stopWordRemovalFile = cl.getResource("partsofspeechfilter.txt").getFile();

        Job job = Job.getInstance(conf, "Wiki Reducer");

        //Set first mapper
        Configuration articleConfig = new Configuration(false);
        DistributedCache.addCacheFile(new File(peopleListFile).toURI(), articleConfig);
        ChainMapper.addMapper(job, GetArticlesMapred.GetArticlesMapper.class, LongWritable.class, WikipediaPage.class, Text.class, Text.class, articleConfig);

        //Set second mapper
        Configuration lemmaIndexConfig = new Configuration(false);
        DistributedCache.addCacheFile(new File(stopWordRemovalFile).toURI(), lemmaIndexConfig);
        ChainMapper.addMapper(job, LemmaIndexMapred.LemmaIndexMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, lemmaIndexConfig);

        //Set third mapper
        Configuration invertIndexConfig = new Configuration(false);
        ChainMapper.addMapper(job, InvertedIndexMapred.InvertedIndexMapper.class, Text.class, IntWritable.class, Text.class, IntWritable.class, invertIndexConfig);

        //Set reducer
        job.setCombinerClass(InvertedIndexMapred.InvertedIndexReducer.class);
        job.setReducerClass(InvertedIndexMapred.InvertedIndexReducer.class);


        job.setJarByClass(InvertedIndexJob.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StringIntegerList.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
