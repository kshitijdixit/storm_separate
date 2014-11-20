package com.wincere.lamda.storm.bolt;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;

public class Subjects {
	
	public void SubjectsName (){
		File xmlFile = new File("/home/neeraj/mapreduce/Subjects.xml");
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		factory.setValidating(true);
		factory.setNamespaceAware(true);
		DocumentBuilder builder;
		try {
			builder = factory.newDocumentBuilder();
			ErrorHandler handler = null ;
		      builder.setErrorHandler(handler);
			//InputSource is = new InputSource();
			//is.setCharacterStream(new StringReader(xmlData));
		      try {
				Document doc = builder.parse(xmlFile);
				NodeList clinicalData = doc.getElementsByTagName("ClinicalData");
				for(int i=0;i<clinicalData.getLength();i++){
					Element clinical = (Element) clinicalData.item(i);
					clinical.getAttribute("StudyOID");
					clinical.getAttribute("MetaDataVersionOID");
					((Element)clinical.getElementsByTagName("SiteRef").item(0)).getAttribute("LocationOID");
					((Element)clinical.getElementsByTagName("SubjectData").item(0)).getAttribute("SubjectKey");
					
					
				}
			} catch (SAXException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
