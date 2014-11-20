package com.wincere.lamda.storm.bolt;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.*;


public class Query {
	
	public void ClinicalDataParse(){
		File xmlFile = new File("/home/neeraj/mapreduce/Audit.xml");
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setValidating(true);
		factory.setNamespaceAware(true);
		
		try {
			DocumentBuilder builder = factory.newDocumentBuilder();
			ErrorHandler handler = null ;
		      builder.setErrorHandler(handler);
			//InputSource is = new InputSource();
			//is.setCharacterStream(new StringReader(xmlData));
			try {
				//Document doc = builder.parse(is);
				Document doc = builder.parse(xmlFile);
				NodeList clinicalData = doc.getElementsByTagName("ClinicalData");
				
				for(int i=0;i<clinicalData.getLength();i++){
					
					Element clinical = (Element) clinicalData.item(i);
					if(clinical.getElementsByTagName("mdsol:Query").getLength()==1){
						
						NodeList node1=clinical.getElementsByTagName("mdsol:Query");
                		Element element1 = (Element) node1.item(0);
						
						clinical.getAttribute("StudyOID");
						((Element)clinical.getElementsByTagName("SiteRef").item(0)).getAttribute("LocationOID");
						((Element)clinical.getElementsByTagName("ItemData").item(0)).getAttribute("ItemOID");
						element1.getAttribute("QueryRepeatKey");
						element1.getAttribute("Value");
						element1.getAttribute("Status");
						element1.getAttribute("Response");
						element1.getAttribute("Recipient");
						((Element)clinical.getElementsByTagName("DateTimeStamp").item(0)).getTextContent();
						if (element1.getAttribute("Status").equals("Closed")){
							((Element)clinical.getElementsByTagName("DateTimeStamp").item(0)).getTextContent();
						}
						if (element1.getAttribute("Status").equals("Answered")){
							((Element)clinical.getElementsByTagName("DateTimeStamp").item(0)).getTextContent();
						}
						
					}
					
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
		Query q = new Query();
		q.ClinicalDataParse();
	}

}
