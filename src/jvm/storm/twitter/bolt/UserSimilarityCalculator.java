package storm.twitter.bolt;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import storm.twitter.Utilities;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class UserSimilarityCalculator extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	final double Threashod;
	int CounterUSer=0;
	OutputCollector _collector;
	public UserSimilarityCalculator(double d) {
		Threashod=d;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) 
	{
		_collector=collector;
	}

	@Override
	public void execute(Tuple Maps) {

		@SuppressWarnings("unchecked")  // first map is user-words hashmap
		Map<String,  Map<String, Integer>> UserTweetVectorMap =(Map<String, Map<String, Integer>>)Maps.getValue(0);
		
		@SuppressWarnings("unchecked")  // second map is word -users inverted hashmap
		Map<String, Set<String>> InvertedWordMap =(Map<String, Set<String>>)Maps.getValue(1);
		
		System.err.println("Second Bolt Map Size:"+UserTweetVectorMap.size());
		System.err.println("Second Bolt InvertedMap Size:"+InvertedWordMap.size());

		Utilities util = new Utilities();
		Map<String, Integer> WordsVectorMapA= new HashMap<String, Integer>();
		Map<String, Integer> WordsVectorMapB= new HashMap<String, Integer>();
		double CosineSim;	
		
		
		List<String> Keys = new ArrayList<String>(UserTweetVectorMap.keySet());
		String[] UserIDs = new String[Keys.size()];
		UserIDs = Keys.toArray(UserIDs);	
		int KeysLength=UserIDs.length;
		String UserIdA;
		String UserIdB;
		Set<String> CandidateUserSetB;
		for (int i=0;i<KeysLength;i++)
		{
			UserIdA=UserIDs[i];
			WordsVectorMapA=UserTweetVectorMap.get(UserIdA);
			CandidateUserSetB=FindCandidateUsers(WordsVectorMapA,InvertedWordMap);
			CandidateUserSetB.remove(UserIdA);  //remove himself
			
			//System.err.println("Before this, we need calculate: "+KeysLength);
			//System.err.println("After this, we just need candidateSize: "+CandidateUserSetB.size());
			for (String candidate : CandidateUserSetB)	
			{
				UserIdB=candidate;
				WordsVectorMapB=UserTweetVectorMap.get(UserIdB);
				
				CosineSim=util.CalculateUserSimilarity(WordsVectorMapA, WordsVectorMapB);
				if (CosineSim>=Threashod)
				{
				    CounterUSer++;
					System.err.println("Number: "+CounterUSer+"*****User A: "+UserIdA+" **UserB: "+UserIdB +" Cosine Similarity: "+(new DecimalFormat("#.#####")).format(CosineSim));
					_collector.emit(new Values(UserIdA,UserIdB,CosineSim));
				}
			}
			
		}

	}

	
	public Set<String> FindCandidateUsers(Map<String, Integer> WordsVectorMap,Map<String, Set<String>> InvertedMap)
	{
		Utilities util = new Utilities();
		Set<String> candidates=new HashSet<String>();
		String word;
		//Map<String, Integer> SortedWordsVectorMap = util.SortHashMapByValue(WordsVectorMap);
		for (Map.Entry<String, Integer> entry : WordsVectorMap.entrySet()) 
		{
			word=entry.getKey();
			candidates.addAll(InvertedMap.get(word));
		}
		
		return candidates;
	}
	
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("UserIdA","UserIdB","CosineSim"));
	}

	public Map<String, Object> getComponentConfiguration() {

		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		//conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Tick_Interval);
		return conf;
	}

}
