package com.wincere.lamda.storm.bolt;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

/**
 * A simple implementation of {@link MongoBolt} which attempts to map the input
 * tuple directly to a MongoDB object.
 *
 * @author Adrian Petrescu <apetresc@gmail.com>
 *
 */
public class SubjectMongoBolt extends MasterTableMongoBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
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
	public SubjectMongoBolt(
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
	public void getDBObjectForInput(Tuple input,String CollectionName,DB mongoDB,OutputCollector collector) {
	//BasicDBObjectBuilder dbObjectBuilder = new BasicDBObjectBuilder();
		BasicDBObject dbObject = new BasicDBObject();
	
		Values values = (Values) input.getValues();
		byte[] message = (byte[]) values.get(0);
		String msg = new String(message);
		System.out.println("We get the data parsing started-----");
		
		File xmlFile = new File("/home/neeraj/mapreduce/study3.xml");
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setValidating(true);
		factory.setNamespaceAware(true);
		
		try {
			DocumentBuilder builder = factory.newDocumentBuilder();
			ErrorHandler handler = null ;
		     builder.setErrorHandler(handler);
			//InputSource is = new InputSource();
			//is.setCharacterStream(new StringReader(msg));
			try {
				Document doc = builder.parse(xmlFile);
				
				
			
				int count =0;
				
				mongoDB.getCollection(CollectionName).insert(dbObject);
				dbObject.clear();
				
				NodeList clinicalData = doc.getElementsByTagName("ClinicalData");
				
				for(int i=0;i<clinicalData.getLength();i++){
					Element clinical = (Element) clinicalData.item(i);
					dbObject.append("StudyOID",clinical.getAttribute("StudyOID"));
					dbObject.append("MetaDataVersionOID",clinical.getAttribute("MetaDataVersionOID"));
					dbObject.append("LocationOID",((Element)clinical.getElementsByTagName("SiteRef").item(0)).getAttribute("LocationOID"));
					dbObject.append("SubjectKey",((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("SubjectKey"));
					
					mongoDB.getCollection("SubjectsList").insert(dbObject);
					dbObject.clear();
					
				}
				
			} catch (SAXException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
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
