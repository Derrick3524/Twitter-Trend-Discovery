package storm.twitter.bolt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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

public class OutputSimilarityScores extends BaseRichBolt {

	private static final long serialVersionUID = 1L;
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {

	}

	@Override
	public void execute(Tuple tuple) 
	{
		String UserIdA=tuple.getValue(0).toString();
		String UserIdB=tuple.getValue(1).toString();
        Double CosineSim=Double.parseDouble(tuple.getValue(2).toString());
        
        
        System.err.println("UserIdA: "+UserIdA+ " UserIdB:"+UserIdB+" CosinSim: "+CosineSim);
        
        FileWriter fstream=null;
        BufferedWriter output=null;
		try
		{
		 fstream = new FileWriter("UserSimilarityOuput.txt", true);
		 output = new BufferedWriter(fstream);
		 DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		 Date date = new Date();
		 System.err.println(dateFormat.format(date));
		 
		 
		 output.newLine();
		 output.write(dateFormat.format(date));
		 output.write(", ");
		 output.write(UserIdA+", "+UserIdB +", Cosine Similarity: "+(new DecimalFormat("#.#####")).format(CosineSim));
//		 output.flush();
		
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			 try {
				output.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	   
		
		
        
		
        
        
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	//	declarer.declare(new Fields("word", "count"));
	}

	public Map<String, Object> getComponentConfiguration() {

		Config conf = new Config();
		conf.setMaxTaskParallelism(1);
		//conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, Tick_Interval);
		return conf;
	}

}

