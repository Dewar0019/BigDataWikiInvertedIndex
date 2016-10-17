package framework.code.lemma;

import framework.util.StringIntegerList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 *
 */
public class LemmaIndexMapred {
    public static class LemmaIndexMapper extends Mapper<Text, Text, Text, StringIntegerList> {
        private NLPUtil tokenizer;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
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
        public void map(Text title, Text contents, Context context) throws IOException, InterruptedException {
            context.write(title, new StringIntegerList(tokenizer.tokenize(contents.toString())));
        }
    }
}
