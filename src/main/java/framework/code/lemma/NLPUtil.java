package framework.code.lemma;

import edu.stanford.nlp.dcoref.CorefChain;
import edu.stanford.nlp.dcoref.CorefCoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.trees.TreeCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;

import java.util.*;

public class NLPUtil {

	private StanfordCoreNLP pipeline;
	private Set<String> wordsToRemove;
	private int longestPhraseEncountered;


	public NLPUtil(Set<String> wordsToRemove, int longestPhraseEncountered) {
		this.wordsToRemove = wordsToRemove;
		this.longestPhraseEncountered = longestPhraseEncountered;
		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, parse, dcoref");
		pipeline = new StanfordCoreNLP(props);
	}

	public Map<String, Integer> tokenize(String text) {

		List<String> wordsList = new ArrayList<>();

// create an empty Annotation just with the given text
		Annotation document = new Annotation(text);

// run all Annotators on this text
		pipeline.annotate(document);


		// these are all the sentences in this document
// a CoreMap is essentially a Map that uses class objects as keys and has values with custom types
		List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

		for (CoreMap sentence : sentences) {
			// a CoreLabel is a CoreMap with additional token-specific methods
			for (CoreLabel token : sentence.get(CoreAnnotations.TokensAnnotation.class)) {
				String lemma = token.get(CoreAnnotations.LemmaAnnotation.class);
				wordsList.add(lemma.toLowerCase());
			}
		}
		return getWordCount(pruneList(wordsList));
	}

	public List<String> pruneList(List<String> toPruneList) {
		List<String> finalWordsList = new ArrayList<>();

		int stopCounter;
		StringBuilder builder = new StringBuilder();
		for(int i =0; i<toPruneList.size(); i++) {

			builder.setLength(0);

			// Start at one because of length of string is not zero indexed
			stopCounter = 1;
			builder.append(toPruneList.get(i)).append(" ");
			String curr = builder.toString().trim();
			//Contains stop word
			if(wordsToRemove.contains(curr)) {
				int j = i+1;
				while(j < toPruneList.size() && stopCounter!= longestPhraseEncountered) {
					builder.append(toPruneList.get(j)).append(" ");
					String subCurr = builder.toString().trim();
					if (wordsToRemove.contains(subCurr)) {
						i=j;
						stopCounter++;
						continue;
					} else {
						break;
					}
				}
			} else {
				//Word is not a stop word
				finalWordsList.add(curr);
			}
		}
		return finalWordsList;
	}


	public Map<String, Integer> getWordCount(List<String> listOfWords) {
		Map<String, Integer> map = new HashMap<>();
		StringBuilder builder = new StringBuilder();
		for(int i =0; i<listOfWords.size(); i++) {

			// Start at one because of length of string is not zero indexed

			String currKey = listOfWords.get(i);

			//Count the first word
			if(map.containsKey(currKey)) {
				int incCount = map.get(currKey) + 1;
				map.put(currKey, incCount);
			} else {
				map.put(currKey, 1);
			}
		}
		return map;
	}
}
