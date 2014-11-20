
package com.wincere.lamda.storm;
import com.wincere.lamda.storm.bolt.VerticaBolt;
import com.wincere.lamda.storm.bolt.VerticaBolt1;
import com.wincere.loadtest.Utility;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import nl.minvenj.nfi.storm.kafka.KafkaSpout;
public class TopologyRunner2 {

	public static void main(String ar[]){
		//Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-spout",new KafkaSpout());
		builder.setBolt("VerticaBolt", new VerticaBolt1()).shuffleGrouping("kafka-spout");
		//Configuration
		Config conf = new Config();
		conf.put("kafka.config","kafka-config.properties");
		//conf.put("consumer.timeout.ms", -1);
		conf.put("kafka.spout.topic", "test5"); // This was not being read from the property 
		conf.put("kafka.spout.buffer.size.max", "1"); // This is too aggressive and unrealstic.	
		conf.setDebug(false);
		//Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Vertica-Storage-Toplogie20", conf, builder.createTopology());
		Utility.delay(10000);
		cluster.shutdown();
	}
}
