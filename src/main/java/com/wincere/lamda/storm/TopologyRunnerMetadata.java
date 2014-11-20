
package com.wincere.lamda.storm;
//import com.wincere.lamda.storm.bolt.MongoBolt;
//import com.wincere.lamda.storm.bolt.SimpleMongoBolt;
//import com.wincere.lamda.storm.bolt.SimpleMongoBolt;
import com.wincere.lamda.storm.bolt.MasterTAbleSimpleMongoBolt;
import com.wincere.lamda.storm.bolt.MasterTableMetadataSimpleMongoBolt;
import com.wincere.lamda.storm.bolt.SimpleMongoBolt;
import com.wincere.lamda.storm.bolt.VerticaBolt;
import com.wincere.lamda.storm.bolt.VerticaBolt1;
import com.wincere.loadtest.Utility;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import nl.minvenj.nfi.storm.kafka.KafkaSpout;
public class TopologyRunnerMetadata{

	public static void main(String ar[]){
		//Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-spout",new KafkaSpout());
		
		//builder.setBolt("VerticaBolt", new VerticaBolt()).shuffleGrouping("kafka-spout");
		builder.setBolt("MongoBolt", new MasterTableMetadataSimpleMongoBolt("172.16.1.186", 27017, "mock2", "metadata")).shuffleGrouping("kafka-spout");
		//Configuration
		//System.setProperty("storm.jar", "/home/himanshu/development/storm_separate/target/storm_separate-1.0-SNAPSHOT-jar-with-dependencies.jar");
		Config conf = new Config();
		
		
		// conf.put(Config.NIMBUS_HOST, "172.16.1.151");
		// conf.put(Config.NIMBUS_THRIFT_PORT,6627);
		    //conf.put(Config.STORM_ZOOKEEPER_PORT,2181);
		  // conf.put(Config.STORM_ZOOKEEPER_SERVERS,"172.16.1.151");
		  //  conf.setNumWorkers(20);
		    //conf.setMaxSpoutPending(5000);
		 
		conf.put("kafka.config","kafka-config.properties");
		
		//conf.put("consumer.timeout.ms", -1);
		conf.put("kafka.spout.topic", "metadata"); // This was not being read from the property 
		conf.put("kafka.spout.buffer.size.max", "1"); // This is too aggressive and unrealstic.	
		conf.setDebug(true);
		
		//Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("Vertica-Storage-Toplogie", conf, builder.createTopology());
		Utility.delay(10000);
		cluster.shutdown();
		 
		//System.setProperty("storm.jar", "/home/neeraj/Developement/storm_separate/target/storm_separate-1.0-SNAPSHOT.jar");
		/*try {
			StormSubmitter.submitTopology("Vertica-Storage-Toplogie2", conf, builder.createTopology());
			//StormSubmitter.submitJar(conf, "/home/neeraj/Developement/storm_separate/target/storm_separate-1.0-SNAPSHOT.jar");
			
			Utility.delay(10000);
			
			
		} catch (AlreadyAliveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
	}
}