package com.wincere.lamda.storm.bolt;

import java.net.UnknownHostException;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class Update {
 
	
	public static void update() throws UnknownHostException {
	       // get handle to "mydb"
		MongoClient mongoClient = new MongoClient();
        DB db = mongoClient.getDB("mydb");

        // get a collection object to work with
        DBCollection coll = db.getCollection("querytest");
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss");
        DBCollection coll1 = db.getCollection("queryaudittest");
	 DBCursor cursor1 = coll.find();
     int count1=0;
     while(cursor1.hasNext())
     		{
     	count1++;
     	System.out.println(count1);
     	BasicDBObject doc1 = (BasicDBObject) cursor1.next();
     	if(!doc1.get("queryStatusID").equals("Open"))
     	{
     		
     		
     		 String queryAnsweredTimeStamp =(String) doc1.get("queryAnsweredTimeStamp");
              String queryResolvedTimeStamp =(String) doc1.get("queryResolvedTimeStamp");
              String queryCreatedTimeStamp = (String) doc1.get("queryCreatedTimeStamp");
              String queryLastUpdatedTimeStamp =(String) doc1.get("queryLastUpdatedTimeStamp");
              System.out.println(queryAnsweredTimeStamp+" "+queryResolvedTimeStamp+" "+queryCreatedTimeStamp+" "+queryLastUpdatedTimeStamp);
              String queryStatusID =(String) doc1.get("queryStatusID");
              String queryRepeatKey =(String) doc1.get("queryRepeatKey");
              DateTime created = formatter.parseDateTime(queryCreatedTimeStamp);

          	   //System.out.println(days);
              if(queryCreatedTimeStamp!="\\N")
              {
           	   if(!queryAnsweredTimeStamp.equals("\\N")) 
           	   {
           	   DateTime answered = formatter.parseDateTime(queryAnsweredTimeStamp);
           	  
           	   int days = Days.daysBetween(created,answered).getDays();
           	   doc1.append("queryResponseTime", days);
           	   
           	   }
              else if(!queryResolvedTimeStamp.equals("\\N")) 
       	   {
              DateTime resolved = formatter.parseDateTime(queryResolvedTimeStamp);
       	   int days = Days.daysBetween(created,resolved).getDays();
       	   doc1.append("queryResponseTime", days);
       
       	   }
              else 
       	   {
              DateTime lastUpdated = formatter.parseDateTime(queryLastUpdatedTimeStamp);
              int days = Days.daysBetween(created,lastUpdated).getDays();
              
       	   doc1.append("queryResponseTime", days);
       	 
       	   
       	   }
              }
              coll1.insert(doc1);
     	}
     		}
     cursor1.close();
     mongoClient.close();
	}
	
}
