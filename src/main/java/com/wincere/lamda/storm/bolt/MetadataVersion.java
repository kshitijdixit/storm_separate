package com.wincere.lamda.storm.bolt;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;

public class MetadataVersion {

	public void getFolderTable(){
		
		File xmlFile = new File("/home/neeraj/mapreduce/Version_Specifications.xml");
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setValidating(true);
		factory.setNamespaceAware(true);
		DocumentBuilder builder;
		try {
			builder = factory.newDocumentBuilder();
			ErrorHandler handler = null ;
		    builder.setErrorHandler(handler);
		    try {
				Document doc = builder.parse(xmlFile);
				NodeList studyNameNode = doc.getElementsByTagName("StudyName");
				String studyname = studyNameNode.item(0).getTextContent();
				System.out.println(studyname);
				
				NodeList versionNodeList = doc.getElementsByTagName("MetaDataVersion");
				Node versionNameNode = versionNodeList.item(0);
				NamedNodeMap versionNamedNodeMap = versionNameNode.getAttributes();
				String versionName = versionNamedNodeMap.item(1).getNodeValue();
				System.out.println(versionName);
			
				// used for maiking folder tables
				
				NodeList folderTable = doc.getElementsByTagName("StudyEventDef");
				
				for (int i=0;i<folderTable.getLength();i++){
					Node folder = folderTable.item(i);
					NamedNodeMap folderInfo = folder.getAttributes();
					if(folderInfo!=null){
						for(int j=0;j<folderInfo.getLength();j++){
							Node nsAttr = folderInfo.item(j);
							//System.out.println(nsAttr.getLocalName()+"  "+nsAttr.getNodeValue());
							
						}
					}
					
				}
				
				// used for making folder tables
				NodeList formTable = doc.getElementsByTagName("FormDef");
				for(int i=0;i<formTable.getLength();i++){
					Node form = formTable.item(i);
					NamedNodeMap formInfo = form.getAttributes();
					if(formInfo!=null){
						for(int j=0;j<formInfo.getLength();j++){
							Node nsAttr = formInfo.item(j);
							//System.out.println(nsAttr.getLocalName()+"  "+nsAttr.getNodeValue());
						}
					}
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
						}
					}
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
						}
					}
				}
				
				// making itemRefItemgroup table
				NodeList itemRefItemGroup = doc.getElementsByTagName("ItemRef");
				System.out.println(itemRefItemGroup.getLength());
				for(int i=0;i<itemRefItemGroup.getLength();i++){
					Node itemRef = itemRefItemGroup.item(i);
					NamedNodeMap itemRefAttr = itemRef.getAttributes();
					Element itemRefElement = (Element) itemRefItemGroup.item(i);
					Element e=(Element)(itemRefElement.getParentNode());
					//System.out.println(itemRefAttr.item(0).getNodeValue()+"   "+e.getAttribute("OID"));
				//goes into itemRefItemGroup table
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
	
	public static void main(String []args){
		MetadataVersion m = new MetadataVersion ();
		m.getFolderTable();
		
	}
}
