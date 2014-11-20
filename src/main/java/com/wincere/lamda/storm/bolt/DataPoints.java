package com.wincere.lamda.storm.bolt;


import java.io.IOException;
import java.io.StringReader;
import java.util.Date;

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
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;

/**
 * A simple implementation of {@link MongoBolt} which attempts to map the input
 * tuple directly to a MongoDB object.
 *
 * @author Adrian Petrescu <apetresc@gmail.com>
 *
 */
public class DataPoints extends MasterTableMongoBolt {
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
	public DataPoints(
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
		
		//File xmlFile = new File("/home/neeraj/mapreduce/Audit2.xml");
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
			
				NodeList clinicalData = doc.getElementsByTagName("ClinicalData");
				
				for(int i=0;i<clinicalData.getLength();i++){
					//System.out.println(clinicalData.getLength());
					Element clinical = (Element) clinicalData.item(i);
					int lock=0;
					int review =0;
					int verify=0;
					int value = 0;
					int item1=0;
					int item2=0;
					if(clinical.getElementsByTagName("mdsol:Review").getLength()>0 || clinical.getElementsByTagName("ItemData").getLength() > 0){
						
						String studyoid = clinical.getAttribute("StudyOID");
						dbObject.append("studyOid", clinical.getAttribute("StudyOID"));
						String locationoid = ((Element)clinical.getElementsByTagName("SiteRef").item(0)).getAttribute("LocationOID");
						dbObject.append("locationoid",((Element)clinical.getElementsByTagName("SiteRef").item(0)).getAttribute("LocationOID"));
						String subjectkey = ((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("SubjectKey");
						dbObject.append("subjectkey", ((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("SubjectKey"));
						String  subjectname = ((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("mdsol:SubjectName");
						dbObject.append("subjectname", ((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("mdsol:SubjectName"));
						
						String studyeventoid = ((Element)clinical.getElementsByTagName("StudyEventData").item(0)).getAttribute("StudyEventOID");
						dbObject.append("studyeventoid", ((Element)clinical.getElementsByTagName("StudyEventData").item(0)).getAttribute("StudyEventOID"));
						
						String studyeventoidrepeatkey = ((Element)clinical.getElementsByTagName("StudyEventData").item(0)).getAttribute("StudyEventRepeatKey");
						dbObject.append("studyeventoidrepeatkey", ((Element)clinical.getElementsByTagName("StudyEventData").item(0)).getAttribute("StudyEventRepeatKey"));
						String formoid =  ((Element)clinical.getElementsByTagName("FormData").item(0)).getAttribute("FormOID");
						dbObject.append("formoid", ((Element)clinical.getElementsByTagName("FormData").item(0)).getAttribute("FormOID"));
						String formrepeatkey = ((Element)clinical.getElementsByTagName("FormData").item(0)).getAttribute("FormRepeatKey");
						dbObject.append("formrepeatkey",((Element)clinical.getElementsByTagName("FormData").item(0)).getAttribute("FormRepeatKey"));
						dbObject.append("updateddatastamp", ((Element)clinical.getElementsByTagName("DateTimeStamp").item(0)).getTextContent());
						
						NodeList node1=clinical.getElementsByTagName("ItemGroupData");
						String itemgroupoid = "\\N";
						String itemgrouprepeatkey = "\\N";
						String itemoid = "\\N";
						//System.out.println(node1.getLength());
						for(int j=0;j<node1.getLength();j++){
							Node nodeItemData= node1.item(j);
							NamedNodeMap nnm = nodeItemData.getAttributes();
							if(nnm!=null){
								for(int k=0;k<nnm.getLength();k++){
									Node nsAttr = nnm.item(k);
									if(nsAttr.getLocalName().equals("ItemGroupOID")){
										item1=1;
										 itemgroupoid = nsAttr.getNodeValue();
										dbObject.append("itemgroupoid",nsAttr.getNodeValue());
									}
									/*else {
										 itemgroupoid = "\\N";
										dbObject.append("ItemGroupOID","\\N");
									}*/
									
									else if(nsAttr.getLocalName().equals("ItemGroupRepeatKey")){
										item2=1;
										 itemgrouprepeatkey = nsAttr.getNodeValue();
										 System.out.println(nsAttr.getNodeValue());
										dbObject.append("itemgrouprepeatkey",nsAttr.getNodeValue());
									}
									/*else 
										{ 
											itemgrouprepeatkey = "\\N";
											dbObject.append("ItemGroupRepeatKey","\\N");
										}*/
									
									//System.out.println(nsAttr.getLocalName()+"   "+nsAttr.getNodeValue());
								}
							}
						}
						if(item1==1 && item2==0){
							itemgrouprepeatkey = "\\N";
							dbObject.append("itemgrouprepeatkey","\\N");
						}
						else if(item1==0 && item2==1){
							itemgroupoid = "\\N";
							dbObject.append("itemgroupoid","\\N");
						}
						
						NodeList node2=clinical.getElementsByTagName("ItemData");
						//System.out.println(node1.getLength());
						for(int j=0;j<node2.getLength();j++){
							Node nodeItemData= node2.item(j);
							NamedNodeMap nnm = nodeItemData.getAttributes();
							if(nnm!=null){
								for(int k=0;k<nnm.getLength();k++){
									Node nsAttr = nnm.item(k);
								
									if(nsAttr.getLocalName().equals("ItemOID")){
									itemoid = nsAttr.getNodeValue();
										
										dbObject.append("itemoid",nsAttr.getNodeValue());
									}	
									
									if(nsAttr.getLocalName().equals("Value")){
										value =1;
										 dbObject.append("value",nsAttr.getNodeValue());
									} 
								
									
									
									if(nsAttr.getLocalName().equals("Lock")){
									 lock =1;
										if(nsAttr.getNodeValue().equals("Yes")){
											dbObject.append("islocked","1");
											dbObject.append("value","\\N");
										}
										else if(nsAttr.getNodeValue().equals("No")){
											dbObject.append("islocked","0");
											dbObject.append("value","\\N");
										}
									}
									else dbObject.append("islocked","\\N");
									
									if(nsAttr.getLocalName().equals("Verify")){
										verify=1;
										if(nsAttr.getNodeValue().equals("Yes")){
											dbObject.append("isverified","1");
											dbObject.append("value","\\N");
										}
										else if(nsAttr.getNodeValue().equals("No")){
											dbObject.append("isverified","0");
											dbObject.append("value","\\N");
										}
									}
									else dbObject.append("isverified","\\N");
									//System.out.println(nsAttr.getLocalName()+"   "+nsAttr.getNodeValue());
								
									
								}
							}
						}
						
						dbObject.append("datapointID",studyoid +"|"+  locationoid+"|"+subjectkey+"|"+subjectname+"|"+studyeventoid+"|"+studyeventoidrepeatkey+ "|"+formoid+"|"+formrepeatkey+"|"+itemgroupoid+"|"+itemgrouprepeatkey+"|"+itemoid);
						
						dbObject.append("DataPageID",studyoid +"|"+  locationoid+"|"+subjectkey+"|"+subjectname+"|"+studyeventoid+"|"+studyeventoidrepeatkey+ "|"+formoid+"|"+formrepeatkey);
						
						dbObject.append("InstanceID",studyoid +"|"+  locationoid+"|"+subjectkey+"|"+subjectname+"|"+studyeventoid+"|"+studyeventoidrepeatkey);
						
						if(clinical.getElementsByTagName("mdsol:Review").getLength()>0){
						NodeList node3=clinical.getElementsByTagName("mdsol:Review");
					
						for(int j=0;j<node3.getLength();j++){
							Node nodeItemData= node3.item(j);
							NamedNodeMap nnm = nodeItemData.getAttributes();
							if(nnm!=null){
								for(int k=0;k<nnm.getLength();k++){
									Node nsAttr = nnm.item(k);
									if(nsAttr.getLocalName().equals("Reviewed")){
										review =1;
										if(nsAttr.getNodeValue().equals("Yes")){
											dbObject.append("isreviewed","1");
											dbObject.append("value","\\N");
										}
										else if(nsAttr.getNodeValue().equals("No")){
											dbObject.append("isreviewed","0");
											dbObject.append("value","\\N");
										}
										
									}
									else dbObject.append("isreviewed","\\N");
									
									//System.out.println(nsAttr.getLocalName()+"   "+nsAttr.getNodeValue());
								}
							}
						}
						
						}else dbObject.append("isreviewed","\\N");
						if((lock==1)||(verify==1)||(review==1)||(value==1)){
							//try{
							//mongoDB.getCollection("DataPoints45").save(dbObject);
								//CreateTableDataPoints crTable = new CreateTableDataPoints();
								//crTable.update(dbObject,collector,input);
							//}catch (MongoException me) {
							//collector.fail(input);
						//}
							
						}
						dbObject.clear();
					}
					
				}
				
			} catch (SAXException | IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			collector.ack(input);
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

