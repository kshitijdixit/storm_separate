package com.wincere.lamda.storm.bolt;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Date;






import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.util.Args;




/**
 * A simple implementation of {@link MongoBolt} which attempts to map the input
 * tuple directly to a MongoDB object.
 *
 * @author Adrian Petrescu <apetresc@gmail.com>
 *
 */
public class SimpleMongoBoltExtend extends MongoBolt {
	private final String mongoCollectionName;

	/**
	 * @param mongoHost The host on which Mongo is running.
	 * @param mongoPort The port on which Mongo is running.
	 * @param mongoDbName The Mongo database containing all collections being
	 * written to.
	 * @param mongoCollectionName The Mongo collection to write to. If a
	 * collection with this name does not already exist, it will be
	 * automatically created.
	 */
	public SimpleMongoBoltExtend(
			String mongoHost, int mongoPort, String mongoDbName, String mongoCollectionName) {

		super(mongoHost, mongoPort, mongoDbName);
		this.mongoCollectionName = mongoCollectionName;
	}


	@Override
	public boolean shouldActOnInput(Tuple input) {
		return true;
	}

	@Override
	public String getMongoCollectionForInput(Tuple input) {
		return mongoCollectionName;
	}

	
	@Override
	public DBObject getDBObjectForInput(Tuple input) {
		BasicDBObjectBuilder dbObjectBuilder = new BasicDBObjectBuilder();
		
		MongoCredential credential = MongoCredential.createMongoCRCredential("superuser", "admin", "12345678".toCharArray());
		 try {
			MongoClient mongoClient = new MongoClient(new ServerAddress("172.16.1.171", 27017), Arrays.asList(credential));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		Values values = (Values) input.getValues();
		byte[] message = (byte[]) values.get(0);
		Object msg = new String(message);
		System.out.println("mongo");
		dbObjectBuilder.append("key", msg);
		
		
	/*	String temp = input.getFields().get(0);
				Object a = temp;
		dbObjectBuilder.append("key", a);
		*/
		
		/*for (String field : input.getFields()) {
			Object value = input.getValueByField(field);
			if (isValidDBObjectField(value)) {
				
				//dbObjectBuilder.append(field, value);
				dbObjectBuilder.append(field, value);
			}
		}*/

		return dbObjectBuilder.get();
	}
	
	private boolean isValidDBObjectField(Object value) {
		return value instanceof String
				|| value instanceof Date
				|| value instanceof Integer
				|| value instanceof Float
				|| value instanceof Double
				|| value instanceof Short
				|| value instanceof Long
				|| value instanceof DBObject;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) { }
}
