package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

public class TextFileReaderSpout extends BaseRichSpout {

  SpoutOutputCollector _collector;
  Random _rand;
  BufferedReader br;
  String line;
  ArrayList<String> sentences;
  int FileSize = 0;
  int SentenceNumber = 0;
  int FinalContro = 0;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
    
    sentences = new ArrayList<String>();
    
    try {
		br =  new BufferedReader(new FileReader("shaks12.txt"));
		int sentenceCount = 0;		
		line = br.readLine();
		
        while (line != null) 
        {        	       	
        	sentences.add(line);
        	sentenceCount++;
        	line = br.readLine();
        }
        FileSize = sentenceCount;
        br.close();	
	} 
    catch (Exception e) 
    {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    
    if(SentenceNumber<FileSize)
    {
    	String currentLine =sentences.get(SentenceNumber);
    	_collector.emit(new Values(currentLine));
    	SentenceNumber++;
    }
    else if(SentenceNumber==FileSize && FinalContro==0){
    	_collector.emit(new Values("90056FinalMarker"));
    	FinalContro++;
    }
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

}