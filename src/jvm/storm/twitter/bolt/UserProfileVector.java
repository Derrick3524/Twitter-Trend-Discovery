package storm.twitter.bolt;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import storm.starter.util.TupleHelpers;
import storm.twitter.Utilities;
import twitter4j.Status;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UserProfileVector extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Map<String, Map<String, Integer>> UserTweetVectorMap = new HashMap<String, Map<String, Integer>>();
	Map<String, Set<String>> InvertedWordMap = new HashMap<String,Set<String>>();
	
	
	int Tick_Interval;
	int CountIDID = 0;
	OutputCollector _collector;

	public UserProfileVector(int i) {
		Tick_Interval=i;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) 
	{
		_collector=collector;
	}

	@Override
	public void execute(Tuple OriginalTweet) {
		String userID;
		String language;
		String tweetText;
		Utilities util = new Utilities();
	//	Map<String, Integer> WordsVectorMap;

		if (TupleHelpers.isTickTuple(OriginalTweet)) 
		{ 
			System.err.println("Is Tick Tuple Triggered!!  Map Size:"+UserTweetVectorMap.size());
			
			Map<String, Map<String, Integer>> UserTweetVectorMap_Copy = new HashMap<String,Map<String, Integer>>(UserTweetVectorMap);
			Map<String, Set<String>> InvertedWordMap_Copy = new HashMap<String,Set<String>>(InvertedWordMap);
			
			_collector.emit(new Values(UserTweetVectorMap_Copy,InvertedWordMap_Copy)); // output a hashmap like [1001;[word1,20;word2,10]]
																					  // and hashmap like [Apple, [user1,user2,user4]]
			
			UserTweetVectorMap=new HashMap<String, Map<String, Integer>>();
			InvertedWordMap = new HashMap<String,Set<String>>();
			System.err.println("Tick Tuple Finished!! Number:"+CountIDID+++" Map Size:"+UserTweetVectorMap.size());
		}

		else {
			Status tweetStatus = (Status) OriginalTweet.getValue(0);
			language = tweetStatus.getUser().getLang();

			if (language.equalsIgnoreCase("en")) {
				tweetText = tweetStatus.getText();
				
				
				String[] tweetWords=null;
				try 
				{
					tweetWords = util.TokenizeTweet(tweetText);
				} 
				catch (IOException e) {
					e.printStackTrace();
				}

				userID=tweetStatus.getUser().getScreenName();
				CreateUserWordsVector(userID,tweetWords);
				CreateInvertedWordList(userID,tweetWords);
			}

		}

		
	}

	
	public void CreateUserWordsVector(String userID, String [] tweetWords)
	{
		Map<String, Integer> WordsVectorMap;
		WordsVectorMap = UserTweetVectorMap.get(userID);
		
		
		if (WordsVectorMap == null) {
			WordsVectorMap = new HashMap<String, Integer>();
		}

		Integer InTweetWordCounter;
		for (String word : tweetWords) 
		{
			InTweetWordCounter = WordsVectorMap.get(word);
			if (InTweetWordCounter == null)
				InTweetWordCounter = 0;
			InTweetWordCounter++;
			WordsVectorMap.put(word, InTweetWordCounter);
		}

		UserTweetVectorMap.put(userID, WordsVectorMap);
		
	}
	
	
	public void CreateInvertedWordList(String userID, String [] tweetWords)
	{
		Set<String> UsersVectorSet;
		for (String word : tweetWords) 
		{
			UsersVectorSet = InvertedWordMap.get(word);
			if (UsersVectorSet==null)
			{
				UsersVectorSet=new HashSet<String>();
			}
			if (!UsersVectorSet.contains(userID))
			{
				UsersVectorSet.add(userID);
			}
			
			InvertedWordMap.put(word, UsersVectorSet);
		}
		
	}
	
	
	
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("UserTweetVectorMap","InvertedWordMap"));
	}

	public Map<String, Object> getComponentConfiguration() {

		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Tick_Interval);
		return conf;
	}

}

