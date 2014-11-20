package com.wincere.lamda.storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

import com.mongodb.BasicDBObject;

import java.text.ParseException;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.sun.corba.se.spi.orbutil.fsm.Guard.Result;

import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Set;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.StringTokenizer;
import java.text.ParseException;

import org.joda.time.DateTime; 
import org.joda.time.Days;



import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;








//import sun.org.mozilla.javascript.internal.json.JsonParser.ParseException;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The tutorial from http://docs.mongodb.org/ecosystem/tutorial/getting-started-with-java-driver/
 */
public class CreateTableDataPoints {

    /**
     * Run this main method to see the output of this quick example.
     *
     * @param args takes no args
     * @throws UnknownHostException if it cannot connect to a MongoDB instance at localhost:27017
     */
    public  void update(BasicDBObject doc , OutputCollector collector,Tuple input) throws UnknownHostException {
        // connect to the local database server
    	
    	MongoCredential credential = MongoCredential.createMongoCRCredential("superuser", "admin", "12345678".toCharArray());
		 try {
			MongoClient mongoClient = new MongoClient(new ServerAddress("172.16.1.171", 27017), Arrays.asList(credential));
		
       // MongoClient mongoClient = new MongoClient("172.16.1.171",27017);

        /*
        // Authenticate - optional
        MongoCredential credential = MongoCredential.createMongoCRCredential(userName, database, password);
        MongoClient mongoClient = new MongoClient(new ServerAddress(), Arrays.asList(credential));
        */
        
        // get handle to "mydb"
        DB db = mongoClient.getDB("UCAPBatchStormTest");

        // get a collection object to work with
        DBCollection coll = db.getCollection("DataPoints1");
       // coll.drop();

        try { 
			    
 	
            BasicDBObject searchQuery = new BasicDBObject().append("datapointID", (String) doc.get("datapointID"));
            BasicDBObject newDocument = new BasicDBObject();
            
            
            DBCursor cursor = coll.find(searchQuery);
        
          
            if(cursor.hasNext())
            {
                DBObject result = cursor.next();
                
                String value = (String) result.get("value");
                String islocked = (String) result.get("islocked");
                String isreviewed = (String) result.get("isreviewed");
                String isverified = (String) result.get("isverified");
                String ItemGroupRepeatKey = (String) result.get("ItemGroupRepeatKey");
            	

               if(doc.get("value").equals("\\N")) {doc.append("value", value); }
               if(doc.get("islocked").equals("\\N")) {doc.append("islocked", islocked); }
               if(doc.get("isreviewed").equals("\\N")) {doc.append("isreviewed", isreviewed); }
               if(doc.get("isverified").equals("\\N")) {doc.append("isverified", isverified); }
              // if(doc.get("ItemGroupRepeatKey").equals("\\N")) {doc.append("isverified", ItemGroupRepeatKey); }
            	
            }                
            
            else
            {
            	if(doc.get("islocked").equals("\\N")) {doc.append("islocked", "0"); }
                if(doc.get("isreviewed").equals("\\N")) {doc.append("isreviewed", "0"); }
                if(doc.get("isverified").equals("\\N")) {doc.append("isverified", "0"); }
            	
             }

            newDocument.append("$set", doc);
            try{
            coll.update(searchQuery,newDocument, true, true );
            }catch (MongoException me) {
			collector.fail(input);
		}
            
          } catch (Exception e) {
            System.err.println("CSV file cannot be read : " + e);
          }

        mongoClient.close();
		 }catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			 e.printStackTrace();
    }

}}