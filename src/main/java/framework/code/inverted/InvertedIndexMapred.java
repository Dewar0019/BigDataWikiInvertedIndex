package framework.code.inverted;

import framework.util.StringIntegerList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * This class is used for Section C.2 of assignment 1. You are supposed to run
 * the code taking the lemma index filename as input, and output being the
 * inverted index.
 */
public class InvertedIndexMapred {
	public static class InvertedIndexMapper extends Mapper<Text, StringIntegerList, Text, StringIntegerList.StringInteger> {

		@Override
		public void map(Text articleId, StringIntegerList indices, Context context) throws IOException,
				InterruptedException {
			// TODO: You should implement inverted index mapper here
		}
	}

	public static class InvertedIndexReducer extends Reducer<Text, StringIntegerList.StringInteger, Text, StringIntegerList> {

		@Override
		public void reduce(Text lemma, Iterable<StringIntegerList.StringInteger> articlesAndFreqs, Context context)
				throws IOException, InterruptedException {
			// TODO: You should implement inverted index reducer here
		}
	}


}
