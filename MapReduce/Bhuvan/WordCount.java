package stormpractice;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.*;
import backtype.storm.topology.*;
import backtype.storm.tuple.*;

public class WordCount {
	public static class RandomSentenceSpout implements IRichSpout {
	    OutputCollector _collector;

	    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	        _collector = collector;
	    }

	    public void cleanup() {
	    	
	    }

	    public void declareOutputFields(OutputFieldsDeclarer declarer) {
	        declarer.declare(new Fields("word"));
	    }
	    
	    public Map getComponentConfiguration() {
	        return null;
	    }

		public void open(Map conf, TopologyContext context,
				SpoutOutputCollector collector) {
			// TODO Auto-generated method stub
			
		}

		public void close() {
			// TODO Auto-generated method stub
			
		}

		public void activate() {
			// TODO Auto-generated method stub
			
		}

		public void deactivate() {
			// TODO Auto-generated method stub
			
		}

		public void nextTuple() {
			// TODO Auto-generated method stub
			String[] randomS=new String[10];
			randomS[0]="Jaidhar gives only 2 marks the 00 and the 1";
			randomS[1]="he says the not a the number";
			randomS[2]="he says the dos the box";
			randomS[3]="he says the twenty the four for twenty four";
			randomS[4]="he says the bread the board";
			randomS[5]="he calls by our register number";
			randomS[6]="he is moody";
			randomS[7]="he never conduts labs properly";
			randomS[8]="he wants feedback from everyone";
			randomS[9]="he is nautanki saala";
			int r=(int) (Math.random()%10);
			_collector.emit(new Values(randomS[r]));
		}

		public void ack(Object msgId) {
			// TODO Auto-generated method stub
			
		}

		public void fail(Object msgId) {
			// TODO Auto-generated method stub
			
		}
	}
	public static class WCMapBolt implements IRichBolt{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		OutputCollector _collector;
		//Map<String, Integer> counts = new HashMap<String, Integer>();
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// TODO Auto-generated method stub
			_collector=collector;
		}

		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			String in=input.getString(0);
			//Integer c=counts.get(word);
			String[] tokens=in.split(" ");
			for(String word:tokens){
				_collector.emit(new Values(word));
			}
		}

		public void cleanup() {
			// TODO Auto-generated method stub
			
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("word"));
		}

		public Map<String, Object> getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	public static class WCReduceBolt implements IRichBolt{
		OutputCollector _collector;
		HashMap<String, Integer> counts = new HashMap<String, Integer>();
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// TODO Auto-generated method stub
			_collector=collector;
		}

		public void execute(Tuple input) {
			// TODO Auto-generated method stub
			String in=input.getString(0);
			Integer c=counts.get(in);
			if(c==null){
				c=0;
			}
			else{
				c++;
			}
			counts.put(in, c);
			_collector.emit(new Values(in,c));
		}

		public void cleanup() {
			// TODO Auto-generated method stub
			try {
				File f = new File("StormWordCount.txt");
				if(f.exists()==false){
					f.createNewFile();
				}
				FileWriter fs = new FileWriter(f);
				@SuppressWarnings("resource")
				BufferedWriter out=new BufferedWriter(fs);
				for(String w:counts.keySet()){
					out.write(w+":"+counts.get(w));
				}
				out.flush();
				out.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub
			declarer.declare(new Fields("word","count"));
		}

		public Map<String, Object> getComponentConfiguration() {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	public static void main(String[] args) throws Exception {

	    TopologyBuilder builder = new TopologyBuilder();

	    builder.setSpout("spout", new RandomSentenceSpout(), 5);

	    builder.setBolt("MAPPER", new WCMapBolt(), 8).shuffleGrouping("spout");
	    builder.setBolt("REDUCER", new WCReduceBolt(), 12).fieldsGrouping("MAPPER", new Fields("word"));

	    Config conf = new Config();
	    conf.setDebug(true);

	    if (args != null && args.length > 0) {
	      conf.setNumWorkers(3);

	      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
	    }
	    else {
	      conf.setMaxTaskParallelism(3);

	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("word-count", conf, builder.createTopology());

	      Thread.sleep(10000);

	      cluster.shutdown();
	    }
	  }
}

