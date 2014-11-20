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
public class QueryMongoBolt extends MasterTableMongoBolt {
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
	public QueryMongoBolt(
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
				
			/*	NodeList clinicalData = doc.getElementsByTagName("ClinicalData");
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
				dbObject.clear();*/
				
			NodeList clinicalData = doc.getElementsByTagName("ClinicalData");
				
				for(int i=0;i<clinicalData.getLength();i++){
					
					Element clinical = (Element) clinicalData.item(i);
					
					int item1=0;
					int item2=0;
					if(clinical.getElementsByTagName("mdsol:Query").getLength()==1){
						
						NodeList node1=clinical.getElementsByTagName("mdsol:Query");
						
                		Element element1 = (Element) node1.item(0);
                		
                		
                		String studyoid = clinical.getAttribute("StudyOID");
                		String locationoid = ((Element)clinical.getElementsByTagName("SiteRef").item(0)).getAttribute("LocationOID");
                		String subjectkey = ((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("SubjectKey");
                		String  subjectname = ((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("mdsol:SubjectName");
                		String studyeventoid = ((Element)clinical.getElementsByTagName("StudyEventData").item(0)).getAttribute("StudyEventOID");
                		String studyeventoidrepeatkey = ((Element)clinical.getElementsByTagName("StudyEventData").item(0)).getAttribute("StudyEventRepeatKey");
                		String formoid =  ((Element)clinical.getElementsByTagName("FormData").item(0)).getAttribute("FormOID");
                		String formrepeatkey = ((Element)clinical.getElementsByTagName("FormData").item(0)).getAttribute("FormRepeatKey");
                		
                		dbObject.append("StudyOID",clinical.getAttribute("StudyOID")).append("SiteOID",((Element)clinical.getElementsByTagName("SiteRef").item(0)).getAttribute("LocationOID"));
                		
                		dbObject.append("subjectname", ((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("mdsol:SubjectName"));
                		dbObject.append("subjectKey",((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("SubjectKey"));
                		
                		dbObject.append("Queryid","\\N");
                		dbObject.append("queryRepeatKey",element1.getAttribute("QueryRepeatKey"));
                		dbObject.append("queryStatusID",element1.getAttribute("Status"));
                		dbObject.append("queryRecepient", element1.getAttribute("Recipient"));
                		dbObject.append("queryLastUpdatedTimeStamp", ((Element)clinical.getElementsByTagName("DateTimeStamp").item(0)).getTextContent());
                		
                		
                		
                		dbObject.append("FormOID", ((Element)clinical.getElementsByTagName("FormData").item(0)).getAttribute("FormOID"));
						dbObject.append("formrepeatkey",((Element)clinical.getElementsByTagName("FormData").item(0)).getAttribute("FormRepeatKey"));
                		
						dbObject.append("StudyEventOID", ((Element)clinical.getElementsByTagName("StudyEventData").item(0)).getAttribute("StudyEventOID"));
                		dbObject.append("studyeventoidrepeatkey", ((Element)clinical.getElementsByTagName("StudyEventData").item(0)).getAttribute("StudyEventRepeatKey"));
                		dbObject.append("UserRef",((Element)clinical.getElementsByTagName("UserRef").item(0)).getAttribute("UserOID"));
                		
                		NodeList node11=clinical.getElementsByTagName("ItemGroupData");
						String itemgroupoid = "\\N";
						String itemgrouprepeatkey = "\\N";
						
						//System.out.println(node1.getLength());
						for(int j=0;j<node11.getLength();j++){
							Node nodeItemData= node11.item(j);
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
						String itemoid = "\\N";
                		 itemoid =  ((Element)clinical.getElementsByTagName("ItemData").item(0)).getAttribute("ItemOID");
						dbObject.append("itemoid", ((Element)clinical.getElementsByTagName("ItemData").item(0)).getAttribute("ItemOID"));
						
						dbObject.append("datapointID",studyoid +"|"+  locationoid+"|"+subjectkey+"|"+subjectname+"|"+studyeventoid+"|"+studyeventoidrepeatkey+ "|"+formoid+"|"+formrepeatkey+"|"+itemgroupoid+"|"+itemgrouprepeatkey+"|"+itemoid);
						
						dbObject.append("DataPageID",studyoid +"|"+  locationoid+"|"+subjectkey+"|"+subjectname+"|"+studyeventoid+"|"+studyeventoidrepeatkey+ "|"+formoid+"|"+formrepeatkey);
						
						dbObject.append("InstanceID",studyoid +"|"+  locationoid+"|"+subjectkey+"|"+subjectname+"|"+studyeventoid+"|"+studyeventoidrepeatkey);
						
						
						
                		if(!element1.getAttribute("Value").equals(null)){
                			dbObject.append("queryValue", element1.getAttribute("Value"));
                		}
                		else{
                			dbObject.append("queryValue", "\\N");
                		}
                		if(!element1.getAttribute("Response").equals("")){
                			dbObject.append("queryResponse", element1.getAttribute("Response"));
                		}
                		else{
                			dbObject.append("queryResponse", "\\N");
                		}
						if (element1.getAttribute("Status").equals("Closed")){
							dbObject.append("queryResolvedTimeStamp", ((Element)clinical.getElementsByTagName("DateTimeStamp").item(0)).getTextContent());
						}
						else{
							dbObject.append("queryResolvedTimeStamp", "\\N");
						}
						if (element1.getAttribute("Status").equals("Answered")){
							dbObject.append("queryAnsweredTimeStamp", ((Element)clinical.getElementsByTagName("DateTimeStamp").item(0)).getTextContent());
						}
						
						else{
							dbObject.append("queryAnsweredTimeStamp","\\N");
						}
						if(element1.getAttribute("Status").equals("Open")){
							dbObject.append("queryCreatedTimeStamp",((Element)clinical.getElementsByTagName("DateTimeStamp").item(0)).getTextContent() );
						}
						
						else dbObject.append("queryCreatedTimeStamp", "\\N");
						//mongoDB.getCollection("Queries5").save(dbObject);
						CreateTable cr = new CreateTable();
						cr.update(dbObject,collector,input);
						dbObject.clear();
					}
					
					/*if(clinical.getAttribute("mdsol:AuditSubCategoryName").equals("EnteredEmpty")||clinical.getAttribute("mdsol:AuditSubCategoryName").equals("EnteredNonConformant")||clinical.getAttribute("mdsol:AuditSubCategoryName").equals("EnteredWithChangeCode")||
							clinical.getAttribute("mdsol:AuditSubCategoryName").equals("Entered")||clinical.getAttribute("mdsol:AuditSubCategoryName").equals("Lock")||clinical.getAttribute("mdsol:AuditSubCategoryName").equals("Review")||clinical.getAttribute("mdsol:AuditSubCategoryName").equals("Verify")
							||clinical.getAttribute("mdsol:AuditSubCategoryName").equals("UnLock")||clinical.getAttribute("mdsol:AuditSubCategoryName").equals("UnVerify")||clinical.getAttribute("mdsol:AuditSubCategoryName").equals("UnReview")){
					
						
					dbObject.append("studyOid", clinical.getAttribute("StudyOID"));
					dbObject.append("locationoid",((Element)clinical.getElementsByTagName("SiteRef").item(0)).getAttribute("LocationOID"));
					dbObject.append("subjectkey", ((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("SubjectKey"));
					dbObject.append("subjectname", ((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("mdsol:SubjectName"));
					dbObject.append("datapointID", ((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("SubjectKey")+((Element)clinical.getElementsByTagName("ItemData").item(0)).getAttribute("ItemOID"));
					
					dbObject.append("studyeventoid", ((Element)clinical.getElementsByTagName("StudyEventData").item(0)).getAttribute("StudyEventOID"));
					dbObject.append("formoid", ((Element)clinical.getElementsByTagName("FormData").item(0)).getAttribute("FormOID"));
					dbObject.append("formrepeatkey",((Element)clinical.getElementsByTagName("FormData").item(0)).getAttribute("FormRepeatKey"));
					dbObject.append("itemgroupoid", ((Element)clinical.getElementsByTagName("ItemGroupData").item(0)).getAttribute("ItemGroupOID"));
					dbObject.append("itemgrouprepeatkey", ((Element)clinical.getElementsByTagName("ItemGroupData").item(0)).getAttribute("ItemGroupRepeatKey"));
					dbObject.append("itemoid", ((Element)clinical.getElementsByTagName("ItemData").item(0)).getAttribute("ItemOID"));
					dbObject.append("updateddatastamp", ((Element)clinical.getElementsByTagName("DateTimeStamp").item(0)).getTextContent());
					//dbObject.append("value", ((Element)clinical.getElementsByTagName("ItemData").item(0)).getAttribute("Value"));
					
					
					
					if(((Element)clinical.getElementsByTagName("ItemData").item(0)).getAttribute("Value").equals("")){
						dbObject.append("value", "\\N");

					}
					else dbObject.append("value", ((Element)clinical.getElementsByTagName("ItemData").item(0)).getAttribute("Value"));

					
					if(clinical.getAttribute("AuditSubCategoryName").equals("Lock")){
						dbObject.append("islocked", "1");
					}
					else if(clinical.getAttribute("AuditSubCategoryName").equals("UnLock")){
						dbObject.append("islocked", "0");
					}
					else {
						dbObject.append("islocked", "\\N");
					}
					if(clinical.getAttribute("AuditSubCategoryName").equals("Review")){
						dbObject.append("isreviewed", "1");
					}
					else if(clinical.getAttribute("AuditSubCategoryName").equals("UnReview")){
						dbObject.append("isreviewed", "0");
					}
					else {
						dbObject.append("isreviewed", "\\N");
					}
					if(clinical.getAttribute("AuditSubCategoryName").equals("Verify")){
						dbObject.append("isverified", "1");
					}
					else if(clinical.getAttribute("AuditSubCategoryName").equals("UnVerify")){
						dbObject.append("isverified", "0");
					}
					else {
						dbObject.append("isverified", "\\N");
					}
					//mongoDB.getCollection("DataPoints3").insert(dbObject);
					CreateTableDataPoints crTable = new CreateTableDataPoints();
					crTable.update(dbObject);
					dbObject.clear();
					}*/
				
			}
				  collector.ack(input);
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
