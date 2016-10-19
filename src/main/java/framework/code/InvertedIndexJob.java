//package framework.code;
//
//import framework.code.GetArticlesMapred;
//import framework.code.InvertedIndexMapred;
//import framework.code.LemmaIndexMapred;
//import framework.util.StringIntegerList;
//import framework.util.WikipediaPageInputFormat;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.util.ToolRunner;
//
//import java.io.File;
//
///**
// * Created by dewartan on 10/15/16.
// */
//public class InvertedIndexJob extends Configured implements Tool {
//
//    private static final String OUTPUT_PATH = "intermediate_output";
//    private static final String OUTPUT_PATH_TWO = "intermediate_output_two";
//
//    public static void main(String[] args) throws Exception {
//
//        System.out.println("Starting Program Inverted Index Program");
//
////        Configuration conf = new Configuration();
////        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
////        if (otherArgs.length != 2) {
////            System.err.println("Usage: invertedIndex <in> <out>");
////            System.exit(2);
////        }
////
////        Path inputPath = new Path(otherArgs[0]);
////        Path outputPath = new Path(otherArgs[1]);
////
////        //Grab the resource files
////        ClassLoader cl = InvertedIndexJob.class.getClassLoader();
////        String peopleListFile = cl.getResource("people.txt").getFile();
////        String stopWordRemovalFile = cl.getResource("partsofspeechfilter.txt").getFile();
////
////        Job job = Job.getInstance(conf, "Wiki Reducer");
////        job.setInputFormatClass(WikipediaPageInputFormat.class);
////        job.setOutputFormatClass(TextOutputFormat.class);
////        //Set first mapper
////
////        JobConf articleConfig = new JobConf(false);
////        DistributedCache.addCacheFile(new File(peopleListFile).toURI(), articleConfig);
////        ChainMapper.addMapper(job, GetArticlesMapred.GetArticlesMapper.class, LongWritable.class, WikipediaPage.class, Text.class, Text.class, articleConfig);
////
////        //Set second mapper
////        JobConf lemmaIndexConfig = new JobConf(false);
////        DistributedCache.addCacheFile(new File(stopWordRemovalFile).toURI(), lemmaIndexConfig);
////        ChainMapper.addMapper(job, LemmaIndexMapred.LemmaIndexMapper.class, Text.class, Text.class, Text.class, StringIntegerList.class, lemmaIndexConfig);
////
////        //Set third mapper
////        JobConf invertIndexConfig = new JobConf(false);
////        ChainMapper.addMapper(job, InvertedIndexMapred.InvertedIndexMapper.class, Text.class, StringIntegerList.class, Text.class, StringIntegerList.class, invertIndexConfig);
////
////        //Set reducer
////        job.setCombinerClass(InvertedIndexMapred.InvertedIndexReducer.class);
////        job.setReducerClass(InvertedIndexMapred.InvertedIndexReducer.class);
////
////
////        job.setJarByClass(InvertedIndexJob.class);
////
////
////        FileInputFormat.addInputPath(job, inputPath);
////        FileOutputFormat.setOutputPath(job, outputPath);
////
////        System.exit(job.waitForCompletion(true) ? 0 : 1);
//
//
//        ToolRunner.run(new Configuration(), new InvertedIndexJob(), args);
//    }
//
//    @Override
//    public int run(String[] args) throws Exception {
//
//
//        Path inputPath = new Path(args[0]);
//
//
//        ClassLoader cl = InvertedIndexJob.class.getClassLoader();
//        String peopleListFile = cl.getResource("people.txt").getFile();
//        String stopWordRemovalFile = cl.getResource("partsofspeechfilter.txt").getFile();
//
//        System.out.println("Running First Job");
//
//        //First Job
//        Configuration conf = new Configuration();
//        DistributedCache.addCacheFile(new File(peopleListFile).toURI(), conf);
//        DistributedCache.addCacheFile(new File(stopWordRemovalFile).toURI(), conf);
//
//        Job job = new Job(conf, "GetArticlesMapReduce");
//        job.setJarByClass(InvertedIndexJob.class);
//
//        job.setMapperClass(GetArticlesMapred.GetArticlesMapper.class);
//
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(Text.class);
//
//        job.setInputFormatClass(WikipediaPageInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//        TextInputFormat.addInputPath(job, inputPath);
//        TextOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
//
//        job.waitForCompletion(true);
//
//        System.out.println("Running Second Job");
//
//
//        //Second job
//        Job jobTwo = new Job(conf, "LemmaIndexMapReduce");
//        jobTwo.setJarByClass(InvertedIndexJob.class);
//
//        jobTwo.setMapperClass(LemmaIndexMapred.LemmaIndexMapper.class);
//
//        jobTwo.setOutputKeyClass(Text.class);
//        jobTwo.setOutputValueClass(StringIntegerList.class);
//
//        jobTwo.setInputFormatClass(TextInputFormat.class);
//        jobTwo.setOutputFormatClass(TextOutputFormat.class);
//
//
//        TextInputFormat.addInputPath(jobTwo, new Path(OUTPUT_PATH));
//        TextOutputFormat.setOutputPath(jobTwo, new Path(OUTPUT_PATH_TWO));
//
//        jobTwo.waitForCompletion(true);
//
//
//        //Third job
//        Job jobThree = new Job(conf, "InvertedIndexMapReduce");
//        jobThree.setJarByClass(InvertedIndexJob.class);
//
//        jobThree.setMapperClass(InvertedIndexMapred.InvertedIndexMapper.class);
//        jobThree.setCombinerClass(InvertedIndexMapred.InvertedIndexReducer.class);
//        jobThree.setReducerClass(InvertedIndexMapred.InvertedIndexReducer.class);
//
//        jobThree.setOutputKeyClass(Text.class);
//        jobThree.setOutputValueClass(StringIntegerList.class);
//
//        jobThree.setInputFormatClass(TextInputFormat.class);
//
//        TextInputFormat.addInputPath(jobThree, new Path(OUTPUT_PATH_TWO));
//        TextOutputFormat.setOutputPath(jobThree, new Path(args[1]));
//
//        return jobThree.waitForCompletion(true) ? 1: 0;
//
//    }
//}
