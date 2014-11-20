
package com.wincere.lamda.storm.bolt;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.wincere.loadtest.Utility;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class VerticaBolt1 implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8288000420611131966L;

	private OutputCollector collector;

	
	@Override
	public void cleanup() {
		// TBD.
		Utility.log(" cleanup is getting called ");
		// Release the Connection.
	}

	@Override
	public void execute(Tuple input) {
		
		//System.out.println(input);
		
		Values values = (Values) input.getValues();
		byte[] message = (byte[]) values.get(0);
		Object word = new String(message);
		System.out.println(word);
		//List <Tuple>a = new ArrayList<Tuple>();
		//a.add(input);
		collector.emit(values);
		
		collector.ack(input);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {	
		this.collector = collector;
		// Get the Pool Configuration.
		Utility.log(" Preparing ");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TBD.
		declarer.declare(new Fields("values"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
