package storm.twitter.bolt;

import java.util.ArrayList;
import java.util.Arrays;
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

public class UserTweetTrend extends BaseRichBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Map<String, Integer> UserCountMap = new HashMap<String,Integer>();

	int Tick_Interval = 300;
	final int TopBuffer=1000;
	final int TopK=10;
	int CountIDID=0;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple OriginalTweet) {

		Utilities util = new Utilities();
		String userID;
		String language;
		if (TupleHelpers.isTickTuple(OriginalTweet)) 
		{

			Map<String, Integer> TopKUsersMap = util.SortHashMapByValue(UserCountMap);
			int TopKCount = 1;

			for (Map.Entry<String, Integer> entry : TopKUsersMap.entrySet()) 
			{
				if (TopKCount > TopK)
					break;
				TopKCount++;
				System.err.println("****UserID:" + entry.getKey()
						+ "*** TweetCount: " + entry.getValue()
					   +" This is user number :"+(CountIDID));
				
			}
			System.err.println("############################## Map size:"+UserCountMap.size()+"##############################");
			CountIDID++;
		} 
		
		else 
		{
			Status tweetStatus = (Status) OriginalTweet.getValue(0);
			language = tweetStatus.getUser().getLang();
			//SpaceSavingTopK

			if (language.equalsIgnoreCase("en")) 
			{

				userID = Long.toString(tweetStatus.getUser().getId());
				util.SpaceSavingTopK(UserCountMap, userID, TopBuffer);

			}

		}

		// collector.emit(new Values(word, count));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

	public Map<String, Object> getComponentConfiguration() {

		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Tick_Interval);
		return conf;
	}

}
