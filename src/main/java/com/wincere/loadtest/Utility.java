
package com.wincere.loadtest;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;

public class Utility {
	
	public static final CharSequence DOT = ".";
	public static final int SIMULATE_USER_COUNT =10;
	public static final String SERVER_ADDRESS ="172.16.1.219"; //"localhost"; test stream listener ip
	public static final int SERVER_PORT = 32000;
	public static final CharSequence PACKET_TERMINATOR_DATA = "-";
	public static final String SIMULATOR_PROP_FILE_NAME = "simulator.properties";
	public static final int PACKET_DELAY_SECONDS = 1;
	public static final CharSequence PLACE_HOLDER = "$";
	public static final int OFF_SET = 0;

	/*
	public static Simulator loadClass(String simulatorName) {
		Object obj = null;
		try {
			obj = Class.forName(simulatorName).newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}		
		return (Simulator)obj;
	}
	*/

	public static void log(String message) {
		System.out.println(message);		
	}

	public static void delay(int i) {
		try {
			Thread.sleep(i*1000);
		} catch (InterruptedException e) {
		}
	}
	
	public static void delayInMillis(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
		}
	}
	
	public static void main(String ar[]){		
		HashMap<String,String> sensorDataMap = parseSensorData("pid:4814,S:161,D:78",",");
		Set<Entry<String,String>> entries = sensorDataMap.entrySet();
		Iterator<Entry<String,String>> iter = entries.iterator(); 
		Utility.log("------------------------"+sensorDataMap.get("D"));
		while(iter.hasNext())
		{
			Entry entry = iter.next();
			Utility.log(""+entry.getKey()+" "+entry.getValue());
		}
		Utility.log("------------------------");
		/*
		long startTimeInMillis = System.currentTimeMillis();
		long begineTimeInMillis = startTimeInMillis;
		int counter = 0;
		int lastCounter = 0;
		for(int i=0;i<10000;i++){
			counter++;
			//if(counter<15000)
			Utility.delayInMillis(1); // 
			
			long deltaTime = System.currentTimeMillis() - startTimeInMillis;
			if(deltaTime/1000 >= 1){
				Utility.log(deltaTime + " deltaTime seconds "+deltaTime/1000+" couter "+counter+" increment "+(counter-lastCounter));
				startTimeInMillis = System.currentTimeMillis();
				lastCounter = counter;
			}
		}		
		long timeElapsed = System.currentTimeMillis()-begineTimeInMillis;
		Utility.log(timeElapsed+" total time "+timeElapsed/1000);
		*/
	}

	public static HashMap<String,String> parseSensorData(String data,String delimiter) {
		HashMap sensorDataMap = new HashMap();
		StringTokenizer sTokenizer = new StringTokenizer(data,delimiter);
		while(sTokenizer.hasMoreTokens()){
			String delimitedData = sTokenizer.nextToken();
			Utility.log(delimitedData);			
			ArrayList<String> keyValuePair = getDelimitedStringData(delimitedData,":");
			String key = keyValuePair.get(0);
			String value = keyValuePair.get(1);
			sensorDataMap.put(key, value);
		}
		return sensorDataMap;
	}

	private static ArrayList<String> getDelimitedStringData(
			String delimitedData, String delimiter) {
		ArrayList<String> delimitedSet = new ArrayList<String>();
		StringTokenizer sTokenizer = new StringTokenizer(delimitedData,delimiter);
		while(sTokenizer.hasMoreTokens()){
			delimitedSet.add(sTokenizer.nextToken());
		}
		return delimitedSet;		
	}
	
	public static String getDate(String dateFormatPattern){
		DateFormat dateFormat = new SimpleDateFormat(dateFormatPattern); // "yyyy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		return dateFormat.format(cal.getTime());
	}
}
