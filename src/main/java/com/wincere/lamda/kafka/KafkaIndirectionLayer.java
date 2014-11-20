package com.wincere.lamda.kafka;

import java.util.Properties;

import com.wincere.lamda.IndirectionLayer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

//https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+Producer+Example
public class KafkaIndirectionLayer implements IndirectionLayer {

	Producer<String, String> producer;
	
	public KafkaIndirectionLayer() {	
		Properties props = new Properties();	 
		props.put("metadata.broker.list", "172.16.1.219:9092");		
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		//props.put("partitioner.class", "example.producer.SimplePartitioner");
		props.put("request.required.acks", "1");	 
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}
	
	@Override
	public void addData(String allData) {
		KeyedMessage<String, String> data = new KeyedMessage<String, String>("test", allData);
		producer.send(data);
		producer.close();
	}
/*public static void main(String []args){
	KafkaIndirectionLayer kf = new KafkaIndirectionLayer();
	kf.addData("neeraj");
}*/
}
