package storm.twitter.bolt;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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

public class TweetWordTrend extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	String tweetText;
	String language;
	int tweetID = 0;
	int CountIDID=0;
	int Tick_Interval = 15;
	final int TopBuffer=999;
	final int TopK=20;

	Map<String, Integer> globalWordCountMap = new HashMap<String, Integer>();
	Map<String, Integer> globalTweetCountMap = new HashMap<String, Integer>();

	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		_collector = collector;
	}

	public void execute(Tuple OriginalTweet) {

		Utilities util = new Utilities();
		if (TupleHelpers.isTickTuple(OriginalTweet)) {
			
			Map<String, Integer> TopKWordsMap = util.SortHashMapByValue(globalWordCountMap);
			int TopKCount = 1;

			for (Map.Entry<String, Integer> entry : TopKWordsMap.entrySet()) {
				if (TopKCount > TopK)
					break;
				TopKCount++;
				System.err.println("****Word:" + entry.getKey()
						+ "*** Word-Count: " + entry.getValue()
						+ "***Tweet-Count:"
						+ globalTweetCountMap.get(entry.getKey())+" This is number :"+(CountIDID));
				
			}
			System.err.println("##############################Map size:"+globalWordCountMap.size());
			CountIDID++;
			
		}

		else {
			Status tweetStatus = (Status) OriginalTweet.getValue(0);
			language = tweetStatus.getUser().getLang();
			Map<String, Integer> INTweetCountMap = new HashMap<String, Integer>();

			if (language.equalsIgnoreCase("en")) {

				// tweetID++;
				tweetText = tweetStatus.getText();

				
				
				String[] tweetWords=null;
				try {
					tweetWords = util.TokenizeTweet(tweetText);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				for (String word : tweetWords) 
				{
					word = word.toLowerCase();

					Integer INWordCounter = INTweetCountMap.get(word);
					if (INWordCounter == null) 
						INWordCounter=0;
					INWordCounter++;
					INTweetCountMap.put(word, INWordCounter);
				}

				String uniqueWordInTweet;
				Integer uniqueCountInTweet;
				for (Map.Entry<String, Integer> entry : INTweetCountMap.entrySet()) {
					uniqueWordInTweet = entry.getKey();
					uniqueCountInTweet=entry.getValue();
					
			    util.CustomSpaceSavingTopK(globalWordCountMap,globalTweetCountMap, uniqueWordInTweet, uniqueCountInTweet,TopBuffer);
					
					
				}
				// _collector.emit(new Values(tweetStatus));
			}
			
			
		}

		// System.err.println("TryTryTweet:::::::::" + tweetStatus.getText());

		// _collector.ack(OriginalTweet);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Word", "tweetID"));
	}

	
	public Map<String, Object> getComponentConfiguration() {

		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Tick_Interval);
		return conf;
	}

}
