package framework.code.articles;

import edu.umd.cloud9.collection.wikipedia.WikipediaPage;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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
		public static Set<String> peopleArticlesTitles = new HashSet<String>();
		private Text title = new Text();
		private Text contents = new Text();

		@Override
		protected void setup(Mapper<LongWritable, WikipediaPage, Text, Text>.Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();

			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
			Path savedLocation = null;

			//Search for the people.txt file
			if (cacheFiles != null && cacheFiles.length > 0) {
				for (int i = 0; i < cacheFiles.length; i++) {
					if (cacheFiles[i].toString().contains("people.txt")) {
						savedLocation = cacheFiles[i];
						break;
					}
				}

				System.out.println("File found at location: " + savedLocation.toString());

				BufferedReader reader = new BufferedReader(new FileReader(savedLocation.toString()));
				System.out.println("Start Reading file");
				StringBuilder builder = new StringBuilder();
				while (reader.ready()) {
					builder.setLength(0);

					//Splits the string so that the occupation in parenthesis is kept as one when splitting
					String[] splitString = reader.readLine().split("\\s+(?=[^\\])}]*([\\[({]|$))");
					int counter = 0;
					for (int i = 0; i < splitString.length; i++) {
						//decodes encoded accent characters
						String currString = splitString[i].trim();
						if(currString.contains("%")) {
							currString = URLDecoder.decode(currString, "UTF8");
						}
						char[] checkString = currString.toCharArray();
						if (checkString[0] != '(' && checkString[checkString.length - 1] != ')') {
							builder.append(currString).append(" ");
							counter++;
						}
					}

					if (builder.length() > 0) {
						//Remove all special characters from final string
						peopleArticlesTitles.add(builder.toString().replaceAll("[^0-9\\p{L}\\s]", "").trim().toLowerCase());
					}
				}
			}


		}
		@Override
		public void map(LongWritable offset, WikipediaPage inputPage, Context context) throws IOException, InterruptedException {
			// TODO: You should implement getting article mapper here
			String wikiTitle= inputPage.getTitle().toLowerCase();
//			String[] splitString = title.split("\\s+(?=[^\\])}]*([\\[({]|$))");

			if(peopleArticlesTitles.contains(wikiTitle)) {
				title.set(wikiTitle);
				contents.set(inputPage.getContent());
				context.write(title, contents);
			}
		}
	}


}
