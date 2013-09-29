package storm.twitter;

import backtype.storm.Config;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TwitterSpout extends BaseRichSpout {
	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream _twitterStream;

	public TwitterSpout() {
	}

	@Override
	public void open(Map conf, TopologyContext context,SpoutOutputCollector collector) 
	{
		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				queue.offer(status);
//				System.err.println("TTTTTTTTTTTTTTTTTTTTTTTT:"+status.getUser().getName() + " : " + status.getText());
			}

			@Override
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

			public void onException(Exception ex) {
				// ex.printStackTrace();
			}

		};

		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true);
		cb.setOAuthConsumerKey("your OAuthConsumerKey");
		cb.setOAuthConsumerSecret("your OAuthConsumerSecret");
		cb.setOAuthAccessToken("your OAuthAccessToken");
		cb.setOAuthAccessTokenSecret("your OAuthAccessTokenSecret");
		cb.setUseSSL(true);
		Configuration configuration = cb.build();
	
		_twitterStream = new TwitterStreamFactory(configuration).getInstance();
		_twitterStream.addListener(listener);
		_twitterStream.sample();
	}

	@Override
	public void nextTuple() {
		//Utils.sleep(500);
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(500);
		} else {
			_collector.emit(new Values(ret));
			
		}
	}

	@Override
	public void close() {
		_twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}