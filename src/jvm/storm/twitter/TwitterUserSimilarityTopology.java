package storm.twitter;
import storm.twitter.bolt.*;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class TwitterUserSimilarityTopology {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("TweetSpout", new TwitterSpout(), 5);

		builder.setBolt("UserProfileVector", new UserProfileVector(300), 6).shuffleGrouping("TweetSpout"); 
		builder.setBolt("UserSimilarityCalculator", new UserSimilarityCalculator(0.7), 6).shuffleGrouping("UserProfileVector");
		builder.setBolt("OutputSimilarityScores", new OutputSimilarityScores(), 6).fieldsGrouping("UserSimilarityCalculator", new Fields("UserIdA"));
		
		
		Config conf = new Config();
		conf.setDebug(false);

		if (args != null && args.length > 0) {
			conf.setNumWorkers(2);
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Twitter_Trend", conf,
					builder.createTopology());
			// Thread.sleep(10000);
			// cluster.shutdown();
		}
	}
	
	
}

