package storm.twitter;
import storm.twitter.bolt.*;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class TwitterUserTrendTopology {

	public static void main(String[] args) throws Exception {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("TweetSpout", new TwitterSpout(), 5);
		builder.setBolt("UserTweetTrend", new UserTweetTrend(), 6).shuffleGrouping("TweetSpout");//Question 2			
		
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
