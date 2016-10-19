package framework.code;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import framework.util.StringIntegerList;
import framework.util.WikipediaPageInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 *
 */
public class LemmaIndexMapred {
    public static class LemmaIndexMapper extends Mapper<LongWritable, WikipediaPage, Text, StringIntegerList> {
        private NLPUtil tokenizer;
        private Text title = new Text();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            System.out.println("Begin LemmaIndexMapred setup");

            Configuration conf = context.getConfiguration();

            Set<String> wordsToRemove = new HashSet<>();
            int longestStopPhrase = 0;


            Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
            Path savedLocation = null;

            //Search for the people.txt file
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (int i = 0; i < cacheFiles.length; i++) {
                    if (cacheFiles[i].toString().contains("partsofspeechfilter.txt")) {
                        savedLocation = cacheFiles[i];
                        break;
                    }
                }

                System.out.println("File found at location: " + savedLocation.toString());

                BufferedReader reader = new BufferedReader(new FileReader(savedLocation.toString()));
                System.out.println("Start Reading file");
                while (reader.ready()) {
                    String word = reader.readLine();
                    int currLength = word.split(" ").length;
                    longestStopPhrase = Math.max(longestStopPhrase, currLength);
                    wordsToRemove.add(word);
                }
            }
            tokenizer = new NLPUtil(wordsToRemove, longestStopPhrase);
        }

        @Override
        public void map(LongWritable offSet, WikipediaPage page, Context context) throws IOException, InterruptedException {
            title.set(page.getTitle().toLowerCase().trim() + "@");
            context.write(title, new StringIntegerList(tokenizer.tokenize(page.getContent())));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Starting Program LemmaIndexMapred Program");

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: framework.code.LemmaIndexMapred <in> <out>");
            System.exit(2);
        }

        Path inputPath = new Path(otherArgs[0]);
        Path outputPath = new Path(otherArgs[1]);

        //Grab the resource files
        ClassLoader cl = LemmaIndexMapred.class.getClassLoader();
        String stopWordRemovalFile = cl.getResource("partsofspeechfilter.txt").getFile();

        DistributedCache.addCacheFile(new File(stopWordRemovalFile).toURI(), conf);

        Job job = Job.getInstance(conf, "LemmaIndexMapred");

        job.setJarByClass(LemmaIndexMapred.class);

        job.setMapperClass(LemmaIndexMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StringIntegerList.class);

        job.setInputFormatClass(WikipediaPageInputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
