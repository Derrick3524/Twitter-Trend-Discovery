package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.starter.spout.*;
import storm.twitter.TwitterSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
	
  public static class SplitSentence extends BaseRichBolt  {

	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	 public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	        _collector = collector;
	    }
	 
	 public void execute(Tuple tuple) {
		
		 String tupleString=tuple.getString(0);
		 System.out.print(tupleString);  
		 String[] words=tupleString.split("[\\s]+");
		 
		 
		 if(tupleString.equals("90056FinalMarker"))
		{
			 _collector.emit(new Values("90056FinalMarker"));
		}
		 
		 for (String word : words)
		 {
			 _collector.emit(new Values(word));
		 }
	  
	      // _collector.ack(tuple);
	    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

 
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  public static class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      if (word.equals("90056FinalMarker"))
      {
			System.out.println("Following is a list of the top 5 frequent words.");
			for (Map.Entry<String, Integer> entry : counts.entrySet()) 
			{
				System.out.println(entry.getKey() + ": " + entry.getValue());
			}
		}
      
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("spout", new TwitterSpout(), 5);

    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
  //  builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    Config conf = new Config();
    conf.setDebug(true);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else 
    {
      conf.setMaxTaskParallelism(3);
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

     // cluster.shutdown();
    }
  }
}
