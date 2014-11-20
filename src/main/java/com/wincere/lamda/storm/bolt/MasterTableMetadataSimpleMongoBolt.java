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
public class MasterTableMetadataSimpleMongoBolt extends MasterTableMongoBolt {
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
	public MasterTableMetadataSimpleMongoBolt(
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
		
		//File xmlFile = new File("/home/neeraj/mapreduce/Version_Specifications.xml");
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setValidating(true);
		factory.setNamespaceAware(true);
		DocumentBuilder builder;
		try {
			builder = factory.newDocumentBuilder();
			ErrorHandler handler = null ;
		    builder.setErrorHandler(handler);
		    InputSource is = new InputSource();
			is.setCharacterStream(new StringReader(msg));
		    try {
				Document doc = builder.parse(is);
			
				// used for maiking folder tables
				NodeList studyNameNode = doc.getElementsByTagName("StudyName");
				String studyname = studyNameNode.item(0).getTextContent();
				
				NodeList versionNodeList = doc.getElementsByTagName("MetaDataVersion");
				Node versionNameNode = versionNodeList.item(0);
				NamedNodeMap versionNamedNodeMap = versionNameNode.getAttributes();
				String versionName = versionNamedNodeMap.item(1).getNodeValue();
				
				NodeList folderTable = doc.getElementsByTagName("StudyEventDef");
				
				for (int i=0;i<folderTable.getLength();i++){
					Node folder = folderTable.item(i);
					NamedNodeMap folderInfo = folder.getAttributes();
					if(folderInfo!=null){
						for(int j=0;j<folderInfo.getLength();j++){
							Node nsAttr = folderInfo.item(j);
							//System.out.println(nsAttr.getLocalName()+"  "+nsAttr.getNodeValue());
							dbObject.append(nsAttr.getLocalName(),nsAttr.getNodeValue());
						}
					}
					String Folder = studyname+versionName+"Folder";
					mongoDB.getCollection(Folder).insert(dbObject);
					dbObject.clear();
				}
				
				// used for making forms tables
				NodeList formTable = doc.getElementsByTagName("FormDef");
				for(int i=0;i<formTable.getLength();i++){
					Node form = formTable.item(i);
					NamedNodeMap formInfo = form.getAttributes();
					if(formInfo!=null){
						for(int j=0;j<formInfo.getLength();j++){
							Node nsAttr = formInfo.item(j);
							//System.out.println(nsAttr.getLocalName()+"  "+nsAttr.getNodeValue());
							dbObject.append(nsAttr.getLocalName(),nsAttr.getNodeValue());
						}
					}
					String Forms = studyname+versionName+"Forms";
					mongoDB.getCollection(Forms).insert(dbObject);
					dbObject.clear();
				}
				
				// making the folderform Tables
				NodeList formFolderRef = doc.getElementsByTagName("FormRef");
				//System.out.println(formFolderRef.getLength());
				for(int i=0;i<formFolderRef.getLength();i++){
					Node form = formFolderRef.item(i);
					Element formRefElement = (Element) formFolderRef.item(i);
					Element e=(Element)(formRefElement.getParentNode());
					NamedNodeMap formInfo = form.getAttributes();
					//System.out.println(formInfo.item(0).getNodeValue()+"   "+e.getAttribute("OID"));
					// goes in folderforms
					//dbObject.append(formInfo.item(0).getNodeValue(),e.getAttribute("OID"));
					dbObject.append("FormOID",formInfo.item(0).getNodeValue()).append("FolderOID", e.getAttribute("OID"));
					
					mongoDB.getCollection(studyname+versionName+"FolderForms").insert(dbObject);
					dbObject.clear();
				}
				
				//making the itemGroupTable
				
				NodeList itemGroupTable = doc.getElementsByTagName("ItemGroupDef");
				//System.out.println(itemGroupTable .getLength());
				for(int i=0;i<itemGroupTable.getLength();i++){
					Node itemGroup = itemGroupTable.item(i);
					NamedNodeMap itemAttr = itemGroup.getAttributes();
					if(itemAttr!=null){
						for(int j=0;j<itemAttr.getLength();j++){
							Node nsAttr = itemAttr.item(j);
							//System.out.println(nsAttr.getLocalName()+"  "+nsAttr.getNodeValue());
							// goes in records table
							dbObject.append(nsAttr.getLocalName(),nsAttr.getNodeValue());
						}
					}
					mongoDB.getCollection(studyname+versionName+"Records").insert(dbObject);
					dbObject.clear();
				}
				
				//making the itemGroupFormTable 
				NodeList itemGroupFormTable = doc.getElementsByTagName("ItemGroupRef");
				//System.out.println(itemGroupFormTable .getLength());
				for(int i=0;i<itemGroupFormTable.getLength();i++){
					Node itemGroup = itemGroupFormTable.item(i);
					Element itemRefElement = (Element) itemGroupFormTable.item(i);
					Element e=(Element)(itemRefElement.getParentNode());
					NamedNodeMap n = itemGroup.getAttributes();
					//System.out.println(n.item(0).getNodeValue()+"   "+e.getAttribute("OID"));
					// goes in FormItemGroups table
					//dbObject.append(n.item(0).getNodeValue(),e.getAttribute("OID"));
					dbObject.append("RecordOID",n.item(0).getNodeValue()).append("FORMOID", e.getAttribute("OID"));
					mongoDB.getCollection(studyname+versionName+"FormRecords").insert(dbObject);
					dbObject.clear();
				}
				
				// making the itemtable
				
				NodeList itemTable = doc.getElementsByTagName("ItemDef");
				//System.out.println(itemTable.getLength());
				
				for(int i=0;i<itemTable.getLength();i++){
					Node item = itemTable.item(i);
					NamedNodeMap itemAttr = item.getAttributes();
					if(itemAttr !=null){
						for(int j=0;j<itemAttr.getLength();j++){
							Node nsAttr = itemAttr.item(j);
							//System.out.println(nsAttr.getLocalName()+"  "+nsAttr.getNodeValue());
							// goes into fields table
							dbObject.append(nsAttr.getLocalName(),nsAttr.getNodeValue());
						}
					}
					mongoDB.getCollection(studyname+versionName+"Fields").insert(dbObject);
					dbObject.clear();
				}
				
				// making itemRefItemgroup table
				
				NodeList itemRefItemGroup = doc.getElementsByTagName("ItemRef");
				
			
				for(int i=0;i<itemRefItemGroup.getLength();i++){
					Node itemGroup = itemRefItemGroup.item(i);
					Element itemRefElement = (Element) itemRefItemGroup.item(i);
					Element e=(Element)(itemRefElement.getParentNode());
					NamedNodeMap n = itemGroup.getAttributes();
					//System.out.println(n.item(0).getNodeValue()+"   "+e.getAttribute("OID"));
					// goes in FormItemGroups table
					dbObject.clear();
					dbObject.append("FieldOID",n.item(0).getNodeValue()).append("RecordOID", e.getAttribute("OID"));
					mongoDB.getCollection(studyname+versionName+"RecordFields").insert(dbObject);
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
	


	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) { }
}
