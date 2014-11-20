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


public class Queries {
	
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
				NodeList nl = doc.getElementsByTagName("*");
				System.out.println(nl.item(0).getLocalName());
				System.out.println(nl.item(0).getNamespaceURI());
				int count =0;
				//for(int i = 1;i<nl.getLength();i++){
					
					Node e=nl.item(6);
					System.out.println(nl.item(7).getLocalName()+nl.item(7).getTextContent());
					
					
					NamedNodeMap nnm = e.getAttributes();
					if(nnm!=null){
						System.out.println(nnm.getLength());
						for(int j=0;j<nnm.getLength();j++){
							Node nsAttr = nnm.item(j);
							
							if(nsAttr.getLocalName()!=null)
								System.out.println("hiii");
							System.out.println(nsAttr.getLocalName()+" = "+nsAttr.getNodeValue());
							//System.out.println(nsAttr.getNodeValue());
							//System.out.println(nsAttr.getNamespaceURI());
						
						}
						
					}
					
				//}
				
				
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
		Queries q = new Queries();
		q.ClinicalDataParse();
	}

}
