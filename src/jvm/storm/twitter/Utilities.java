package storm.twitter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;

public class Utilities {

	public String[] TokenizeTweet(String tweet) throws IOException {

		String clearTweet = tweet.replaceAll("[^0-9A-Za-z\\s]", " ");
		       clearTweet=clearTweet.toLowerCase();      
		       
	
		       FileInputStream inputStream = new FileInputStream("stop_words.txt");
		       String stopWordsStream = IOUtils.toString(inputStream);
		       String [] stops=null;
		       stops=stopWordsStream.split(",");
		       String stopWords="a";
		       for (String s : stops) 
		       {
		    	   stopWords+=("|"+s);
		       }
		       inputStream.close();
		       clearTweet = clearTweet.replaceAll("\\b("+stopWords+")\\b", " ");
		       clearTweet = clearTweet.replaceAll("\\b[0-9a-z]\\b", " ");
		String[] wordsInTweet = clearTweet.split("[\\s]+");
		return wordsInTweet;
	}


	public void CustomSpaceSavingTopK(Map<String, Integer> globalWordMap,
			Map<String, Integer> globalTweetMap,
			 String word, Integer count,
			int Buffer) {
		
		Integer wordCounter = globalWordMap.get(word);
		Integer tweetCounter=globalTweetMap.get(word);
		
		if (wordCounter != null) 
		{
			wordCounter+=count;
			tweetCounter++;
			globalWordMap.put(word, wordCounter);
			globalTweetMap.put(word, tweetCounter);
			
		} else if (globalWordMap.size() < Buffer) {
			globalWordMap.put(word, 1);
			globalTweetMap.put(word, 1);
		} else {
			String keyMin = FindKeyOfMinValue(globalWordMap);
			Integer countMin = globalWordMap.get(keyMin);
			countMin++;
			globalWordMap.remove(keyMin);
			globalWordMap.put(word, countMin);
			
			Integer countMinOfTweetCount=globalTweetMap.get(keyMin);
			countMinOfTweetCount++;
			globalTweetMap.remove(keyMin);
			globalTweetMap.put(word, countMinOfTweetCount);
		}

	}
	
	
	public void SpaceSavingTopK(Map<String, Integer>currentGlobalMap, String userID, int K) {
		 Integer userCounter = currentGlobalMap.get(userID);
		 if (userCounter != null)
		 {
		   userCounter++;
		   currentGlobalMap.put(userID, userCounter);
		 } 
		 else if (currentGlobalMap.size() < K) 
		 {
		   currentGlobalMap.put(userID, 1);
		 } else {
		 String keyMin = FindKeyOfMinValue(currentGlobalMap);
		 Integer countMin = currentGlobalMap.get(keyMin);
		 countMin++;
		 currentGlobalMap.remove(keyMin);
		 currentGlobalMap.put(userID, countMin);
		 }
		
	    }
	
	
	public String FindKeyOfMinValue(Map<String, Integer> currentMap) {
		if (currentMap==null || currentMap.isEmpty())
		{
			return null;
		}
		
		Integer min = Collections.min(currentMap.values());
		String key = "";
		for (Map.Entry<String, Integer> entry : currentMap.entrySet()) {
			if (min.equals(entry.getValue())) {
				key = entry.getKey();
				break;
			}
		}

		return key;
	}
	
	
	

	public double HashMapValueLength(Map<String,Integer> Map)
	{
		Integer length=0;
		if (Map==null || Map.isEmpty())
		{
			return 0;
		}
		
		
		for (Map.Entry<String, Integer> entry : Map.entrySet())
		{
			length+=(entry.getValue()*entry.getValue());
		}

		return Math.sqrt(length);
		
	}
	
	
	public double CalculateUserSimilarity(Map<String,Integer> UserMapA,Map<String,Integer> UserMapB )
	{
		if (UserMapA==null || UserMapB ==null)
		{
			return 0;
		}
		
		
		double LengthA;
		double LengthB;
		double CosineSim;
		LengthA=HashMapValueLength(UserMapA);
		LengthB=HashMapValueLength(UserMapB);
		
		Set<String> commonWords = new HashSet<String>(UserMapA.keySet());
		commonWords.retainAll(UserMapB.keySet());
		
		double dotProduct=0;
		for (String Word :commonWords)
		{
			dotProduct+=(UserMapA.get(Word))*(UserMapB.get(Word));
		}
		
		CosineSim=(LengthA*LengthB==0)?0:(dotProduct/(LengthA*LengthB));
		
		return CosineSim;
	}
	
	

	public LinkedHashMap<String, Integer> SortHashMapByValue(Map<String, Integer> MapToSort) {

		Map<String, Integer> copy_MapToSort = new HashMap<String, Integer>(
				MapToSort);

		List<String> mapKeys = new ArrayList<String>(copy_MapToSort.keySet());
		List<Integer> mapValues = new ArrayList<Integer>(
				copy_MapToSort.values());
		Collections.sort(mapValues, Collections.reverseOrder());
		Collections.sort(mapKeys, Collections.reverseOrder());

		LinkedHashMap<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();

		Iterator<Integer> valueIt = mapValues.iterator();
		while (valueIt.hasNext()) {
			Integer val = valueIt.next();
			Iterator<String> keyIt = mapKeys.iterator();

			while (keyIt.hasNext()) {
				String key = keyIt.next();
				String comp1 = copy_MapToSort.get(key).toString();
				String comp2 = val.toString();

				if (comp1.equals(comp2)) {
					copy_MapToSort.remove(key);
					mapKeys.remove(key);
					sortedMap.put(key, val);
					break;
				}

			}

		}
		return sortedMap;
	}

	
	public static void main(String[] args) throws Exception {

//for testing

	}

}



