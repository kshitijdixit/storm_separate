/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
public class CreateTable {
    // CHECKSTYLE:OFF
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
        DB db = mongoClient.getDB("UCAPBatchTest");

        // get a collection object to work with
        DBCollection coll = db.getCollection("Queries1");
      //  DBCollection status = db.getCollection("statustest1");
        //DBCollection coll1 = db.getCollection("queryaudittest1");
        // drop all the data in it
        //coll.drop();
        //status.drop();
        //coll1.drop();
        
       
      /*  status.insert(new BasicDBObject().append("queryStatus", "Open").append("QueryStatusID","1"));
        status.insert(new BasicDBObject().append("queryStatus", "Answered").append("QueryStatusID","2"));
        status.insert(new BasicDBObject().append("queryStatus", "Closed").append("QueryStatusID","3"));
        status.insert(new BasicDBObject().append("queryStatus", "Cancelled").append("QueryStatusID","4")); */
        // make a document and insert it
      
        int count=0;
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
        try { 
			
  
          		  
             
             //.equals("Open")?"1":(splitValue[5].equals("Answered")?"2":"3")
            	
    
 	
            BasicDBObject searchQuery = new BasicDBObject().append("queryRepeatKey", (String) doc.get("queryRepeatKey"));
            BasicDBObject newDocument = new BasicDBObject();
        
            
            DBCursor cursor = coll.find(searchQuery);
            //DBObject result = cursor.next();
        
          
            if(cursor.hasNext())
            {
            	DBObject result = cursor.next();
                
                String queryValue = (String) result.get("queryValue");
            	String queryStatusID = (String) result.get("queryStatusID");
            	String queryResponse = (String) result.get("queryResponse");
            	String queryResolvedTimeStamp = (String) result.get("queryResolvedTimeStamp");
            	String queryAnsweredTimeStamp = (String) result.get("queryAnsweredTimeStamp");
            	String queryCreatedTimeStamp = (String) result.get("queryCreatedTimeStamp");
            	
               if(doc.get("queryValue").equals("\\N")) {doc.append("queryValue", queryValue); }
               if(doc.get("queryStatusID").equals("\\N")) {doc.append("queryStatusID", queryStatusID); }
               if(doc.get("queryResponse").equals("\\N")) {doc.append("queryResponse", queryResponse); }
               if(doc.get("queryResolvedTimeStamp").equals("\\N")) {doc.append("queryResolvedTimeStamp", queryResolvedTimeStamp); }
               if(doc.get("queryAnsweredTimeStamp").equals("\\N")) {doc.append("queryAnsweredTimeStamp", queryAnsweredTimeStamp); }
               doc.append("queryCreatedTimeStamp", queryCreatedTimeStamp);
             }
             if(doc.get("queryStatusID").equals("Open")) doc.append("queryCreatedTimeStamp", doc.get("queryCreatedTimeStamp"));
             
             
           //System.out.println(count);
            newDocument.append("$set", doc);
            try{
            coll.update(searchQuery,newDocument, true, true );
            }catch (MongoException me) {
            	collector.fail(input);
            }
           // collector.ack(input);

            //coll.insert(doc);
            
            
           
            	
            

          } catch (Exception e) {
            System.err.println("CSV file cannot be read : " + e);
          }
		
        //System.out.println(count);
        // lets get all the documents in the collection and print them out
        /*DBCursor cursor = coll1.find();
        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }*/

       /* // now use a query to get 1 document out
        BasicDBObject query = new BasicDBObject("i", 71);
        cursor = coll.find(query);

        try {
            while (cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }*/


        // release resources
        //db.dropDatabase();
        mongoClient.close();
		 } catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    }
    
    // CHECKSTYLE:ON

}
