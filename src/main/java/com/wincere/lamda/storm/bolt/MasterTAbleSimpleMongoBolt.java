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
public class MasterTAbleSimpleMongoBolt extends MasterTableMongoBolt {
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
	public MasterTAbleSimpleMongoBolt(
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
		
		//File xmlFile = new File("/home/neeraj/mapreduce/Audit.xml");
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setValidating(true);
		factory.setNamespaceAware(true);
		
		try {
			DocumentBuilder builder = factory.newDocumentBuilder();
			ErrorHandler handler = null ;
		     builder.setErrorHandler(handler);
			InputSource is = new InputSource();
			is.setCharacterStream(new StringReader(msg));
			try {
				Document doc = builder.parse(is);
				
				NodeList nl = doc.getElementsByTagName("*");
			
				int count =0;
				
				
				for(int i = 1;i<nl.getLength();i++){
				
					Node e=nl.item(i);
					if(nl.item(i).getLocalName()=="ClinicalData") {
						count ++;
						
						if(count >1) {
							
					
							mongoDB.getCollection(CollectionName).insert(dbObject);
							
							//mongoDB.getCollection(CollectionName).save(dbObject,new WriteConcern(1));
							dbObject.clear();
							System.out.println(dbObject.size());
						}
					}
					
					NamedNodeMap nnm = e.getAttributes();
					if(nnm!=null){
						if(nnm.getLength()==0){
							if(nl.item(i).getTextContent()!=null){
								dbObject.append(nl.item(i).getLocalName(),nl.item(i).getTextContent());
							}
						}
						
						for(int j=0;j<nnm.getLength();j++){
							Node nsAttr = nnm.item(j);
						
							
							dbObject.append(nsAttr.getLocalName(),nsAttr.getNodeValue());
							
							
						}
					}
					
					
				}
				
				mongoDB.getCollection(CollectionName).insert(dbObject);
				dbObject.clear();
				
			/*	NodeList clinicalData = doc.getElementsByTagName("ClinicalData");
				
				for(int i=0;i<clinicalData.getLength();i++){
					
					Element clinical = (Element) clinicalData.item(i);
					if(clinical.getElementsByTagName("mdsol:Query").getLength()==1){
						
						NodeList node1=clinical.getElementsByTagName("mdsol:Query");
						
                		Element element1 = (Element) node1.item(0);
                		dbObject.append("Studyoid",clinical.getAttribute("StudyOID")).append("Siteoid",((Element)clinical.getElementsByTagName("SiteRef").item(0)).getAttribute("LocationOID")).append("SubjectKey",((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("SubjectKey")).append("Itemoid", ((Element)clinical.getElementsByTagName("ItemData").item(0)).getAttribute("ItemOID")).append("Queryid","\\N").append("Queryrepeatkey",element1.getAttribute("QueryRepeatKey")).append("Queryvalue", element1.getAttribute("Value")).append("Querystatu",element1.getAttribute("Status"))
                		.append("Queryrecepient", element1.getAttribute("Recipient")).append("Querylastupdatedtimestamp", ((Element)clinical.getElementsByTagName("DateTimeStamp").item(0)).getTextContent());
                		
                		dbObject.append("FormOID", ((Element)clinical.getElementsByTagName("FormData").item(0)).getAttribute("FormOID"));
                		dbObject.append("StudyEventOID", ((Element)clinical.getElementsByTagName("StudyEventData").item(0)).getAttribute("StudyEventOID"));
                		dbObject.append("ItemOID",((Element)clinical.getElementsByTagName("ItemData").item(0)).getAttribute("ItemOID"));
                		
                		if(!element1.getAttribute("Response").equals("")){
                			dbObject.append("Queryresponse", element1.getAttribute("Response"));
                		}
                		else{
                			dbObject.append("Queryresponse", "\\N");
                		}
						if (element1.getAttribute("Status").equals("Closed")){
							dbObject.append("Queryresolvedtimestamp", ((Element)clinical.getElementsByTagName("DateTimeStamp").item(0)).getTextContent());
						}
						else{
							dbObject.append("Queryresolvedtimestamp", "\\N");
						}
						if (element1.getAttribute("Status").equals("Answered")){
							dbObject.append("Queryansweredtimestamp", ((Element)clinical.getElementsByTagName("DateTimeStamp").item(0)).getTextContent());
						}
						
						else{
							dbObject.append("Queryansweredtimestamp","\\N");
						}
						
						mongoDB.getCollection("Queries6").insert(dbObject);
						dbObject.clear();
					}
				
				}*/
				
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
