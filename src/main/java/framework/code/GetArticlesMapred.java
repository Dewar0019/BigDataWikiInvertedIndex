package framework.code;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Set;

/**
 * This class is used for Section A of assignment 1. You are supposed to
 * implement a main method that has first argument to be the dump wikipedia
 * input filename , and second argument being an output filename that only
 * contains articles of people as mentioned in the people auxiliary file.
 */
public class GetArticlesMapred {

    //@formatter:off
    /**
     * Input:
     * 		Page offset 	WikipediaPage
     * Output
     * 		Page offset 	WikipediaPage
     * @author Tuan
     *
     */
    //@formatter:on
    public static class GetArticlesMapper extends Mapper<LongWritable, WikipediaPage, Text, Text> {
        public static Set<String> peopleArticlesTitles = new HashSet<>();
        private Text title = new Text();
        private Text wikiPage = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            System.out.println("Begin GetArticlesMapper setup");

            Configuration conf = context.getConfiguration();

            URI[] cacheFiles = DistributedCache.getCacheFiles(conf);
            URI savedLocation = null;

            //Search for the people.txt file
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (int i = 0; i < cacheFiles.length; i++) {
                    if (cacheFiles[i].toString().contains("people.txt")) {
                        savedLocation = cacheFiles[i];
                        break;
                    }
                }

                System.out.println("File found at location: " + savedLocation.toString());

                BufferedReader reader = new BufferedReader(new FileReader(new File(savedLocation)));
                System.out.println("Start Reading file");
                String currString = "";
                while (reader.ready()) {
                    currString = reader.readLine();
                    if (currString.contains("%")) {
                        currString = URLDecoder.decode(currString, "UTF8");
                    }
                    //Remove all special characters from final string
                    peopleArticlesTitles.add(currString.replaceAll("[^A-Za-z\\p{L}\\s]", "").trim().toLowerCase());
                }
                title.set("");
            }
        }

        @Override
        public void map(LongWritable offset, WikipediaPage inputPage, Context context) throws IOException, InterruptedException {
            String wikiTitle = inputPage.getTitle().toLowerCase();
            if (peopleArticlesTitles.contains(wikiTitle)) {
                wikiPage.set(inputPage.getRawXML());
                context.write(title, wikiPage);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Starting Program Get Articles Mapped Program");

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: framework.code.articles.GetArticlesMapRed <in> <out>");
            System.exit(2);
        }

        Path inputPath = new Path(otherArgs[0]);
        Path outputPath = new Path(otherArgs[1]);

        //Grab the resource files
        ClassLoader cl = GetArticlesMapred.class.getClassLoader();
        String peopleListFile = cl.getResource("people.txt").getFile();

        //First Job
        DistributedCache.addCacheFile(new File(peopleListFile).toURI(), conf);
//            DistributedCache.addCacheFile(new File(stopWordRemovalFile).toURI(), conf);

        Job job = Job.getInstance(conf, "GetArticlesMapred");

        job.setJarByClass(GetArticlesMapred.class);

        job.setMapperClass(GetArticlesMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(WikipediaPageInputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
