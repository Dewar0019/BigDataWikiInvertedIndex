package mapreduce;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import edu.umd.cloud9.collection.wikipedia.WikipediaPageInputFormat;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashSet;


/**
 * PA1 Assignment
 * Counts the number of occurrences in which a name occurs
 * from the people.txt file
 * Dewar Tan, Dewar0019@brandeis.edu
 */

public class NameCount {


    public static class TokenizerMapper extends Mapper<LongWritable, edu.umd.cloud9.collection.wikipedia.WikipediaPage, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private HashSet<String> nameStorage = null;
        private int longestNameLength = 0;

        @Override
        public void setup(Context context) throws IOException {
//            nameStorage = new HashSet<String>();
//            Configuration conf = context.getConfiguration();
//
//            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
//            Path savedLocation = null;
//
//            //Search for the people.txt file
//            if (cacheFiles != null && cacheFiles.length > 0) {
//                for (int i = 0; i < cacheFiles.length; i++) {
//                    if (cacheFiles[i].toString().contains("people.txt")) {
//                        savedLocation = cacheFiles[i];
//                        break;
//                    }
//                }
//
//                System.out.println("File found at location: " + savedLocation.toString());
//
//                BufferedReader reader = new BufferedReader(new FileReader(savedLocation.toString()));
//                System.out.println("Start Reading file");
//                StringBuilder builder = new StringBuilder();
//                while (reader.ready()) {
//                    builder.setLength(0);
//
//                    //Splits the string so that the occupation in parenthesis is kept as one when splitting
//                    String[] splitString = reader.readLine().split("\\s+(?=[^\\])}]*([\\[({]|$))");
//                    int counter = 0;
//                    for (int i = 0; i < splitString.length; i++) {
//                        //decodes encoded accent characters
//                        String currString = splitString[i].trim();
//                        if(currString.contains("%")) {
//                            currString = URLDecoder.decode(currString, "UTF8");
//                        }
//                        char[] checkString = currString.toCharArray();
//                        if (checkString[0] != '(' && checkString[checkString.length - 1] != ')') {
//                            builder.append(currString).append(" ");
//                            counter++;
//                        }
//                    }
//
//                    if (builder.length() > 0) {
//                        longestNameLength = Math.max(longestNameLength, counter);
//                        nameStorage.add(builder.toString().trim().toLowerCase());
//                    }
//                }
//                System.out.println("Longest name size: " + longestNameLength);
//            }
        }

        @Override
        public void map(Object key, WikipediaPage page,  Context context) throws IOException, InterruptedException {
            WikipediaPage page;
//            page.getContent();

            //crop
            //look for name

//            page.getDocid();

            //Converts html into string
            String unEscapedString = StringEscapeUtils.unescapeHtml(value.toString());


            //decodes encoded strings
            if(unEscapedString.contains("%")) {

                //Cleans up encodings so that URLDecoder can decode
                unEscapedString = unEscapedString.replaceAll("%(?![0-9a-fA-F]{2})", "%25");
                unEscapedString = unEscapedString.replaceAll("\\+", "%2B");

                //Decode encodings
                unEscapedString = URLDecoder.decode(unEscapedString, "UTF8");
            }

            //Remove all except characters except alphanumeric and accented characters.
            String[] itr = unEscapedString.toLowerCase().replaceAll("[^0-9\\p{L}\\s]", " ").split(" ");

            int stopCounter;
            StringBuilder builder = new StringBuilder();
            for(int i =0; i<itr.length; i++) {

                builder.setLength(0);

                // Start at one because of length of string is not zero indexed
                stopCounter = 1;
                builder.append(itr[i]).append(" ");
                String curr = builder.toString().trim();
                if(nameStorage.contains(curr)) {
                    word.set(curr);
                    System.out.println("Found Word:" + curr);
                    context.write(word, one);
                }
                for(int j = i+1; j<itr.length; i++) {
                    if(stopCounter == longestNameLength) {
                        break;
                    }
                    builder.append(itr[j]).append(" ");
                    String subCurr = builder.toString().trim();
                    if (nameStorage.contains(subCurr)) {
                        word.set(subCurr);
                        System.out.println("Found Word:" + subCurr);
                        context.write(word, one);
                    }
                    stopCounter++;
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Program Name Counter");

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: namecount <in> <out>");
            System.exit(2);
        }

        ClassLoader cl = NameCount.class.getClassLoader();
        String peopleListFile = cl.getResource("people.txt").getFile();
        String stopWordRemovalFile = cl.getResource("partsofspeechfilter.txt").getFile();
        DistributedCache.addCacheFile(new File(peopleListFile).toURI(), conf);
        DistributedCache.addCacheFile(new File(stopWordRemovalFile).toURI(), conf);


        Job job = Job.getInstance(conf, "Name Counter");



        job.setJarByClass(NameCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setInputFormatClass(WikipediaPageInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }





}
