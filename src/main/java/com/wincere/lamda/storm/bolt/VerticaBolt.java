
package com.wincere.lamda.storm.bolt;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.wincere.loadtest.Utility;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class VerticaBolt implements IRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8288000420611131966L;

	private OutputCollector collector;

	
	@Override
	public void cleanup() {
		// TBD.
		Utility.log(" cleanup is getting called ");
		// Release the Connection.
	}

	@Override
	public void execute(Tuple input) {
		Values values = (Values) input.getValues();
		System.out.println(input.getValue(0));
		byte[] message = (byte[]) values.get(0);
		String msg = new String(message);
		Utility.log("Let us store it now in Vertica"+values.size()+" "+msg);
		HashMap<String,String> sensorDataMap = null;
		try
		{
			sensorDataMap = Utility.parseSensorData(msg,",");
		}
		catch(Exception e){
			Utility.log("Sensor Data is messed up, send a mail to admin@wincer.com");
			collector.ack(input);
			return;
		}
		String patiendId = sensorDataMap.get("pid");
		String diastolicReading = sensorDataMap.get("D");
		// remove eod - from it
		diastolicReading = diastolicReading.substring(0, diastolicReading.length()-1);
		String systolicReading = sensorDataMap.get("S");
		Utility.log("insert into patience_bp_data(patient_id,s_reading,d_reading) values("+patiendId+","+systolicReading+","+diastolicReading+")");
		String insertSQL = "insert into patience_bp_data(patient_id,s_reading,d_reading) values("+patiendId+","+systolicReading+","+diastolicReading+")";
		/*
		Connection con = null;
		try{
			con = getConnection();
			Statement stmt;
			try {
				stmt = con.createStatement();
				stmt.execute(insertSQL);
			} catch (SQLException e) {
				e.printStackTrace();
			}			
		}
		finally{
			try {
				if(con != null && !con.isClosed()){
					con.close();
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}*/
		// Get the Connection from the Pool.
		collector.ack(input);
	}

	private Connection getConnection() {
		Properties myProp = new Properties();
        myProp.put("user", "dbadmin");
        myProp.put("password", "");
        myProp.put("loginTimeout", "35");
        myProp.put("binaryBatchInsert", "true");
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(
                    "jdbc:vertica://172.16.1.233:5433/VMart", myProp);  
        } catch (SQLTransientConnectionException connException) {
            // There was a potentially temporary network error
            // Could automatically retry a number of times here, but
            // instead just report error and exit.
            System.out.print("Network connection issue: ");
            System.out.print(connException.getMessage());
            System.out.println(" Try again later!");
        } catch (SQLInvalidAuthorizationSpecException authException) {
            // Either the username or password was wrong
            System.out.print("Could not log into database: ");
            System.out.print(authException.getMessage());
            System.out.println(" Check the login credentials and try again.");
        } catch (SQLException e) {
            // Catch-all for other exceptions
            e.printStackTrace();
        }
		return conn;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {	
		this.collector = collector;
		// Get the Pool Configuration.
		Utility.log(" Preparing ");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TBD.
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}
