package com.jsoncreate;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.github.wnameless.json.flattener.JsonFlattener;

public class JsonObjectCreation {

	static PreparedStatement stmt = null;
	static ResultSet rs = null;
	static ResultSetMetaData rsmd = null;
	static JSONObject json = new JSONObject();
	static JSONArray jsonarray = new JSONArray();
	static Map<String, Object> flattenJson;
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, JSONException, ParseException {
		// TODO Auto-generated method stub
		ConnectionMysql();

	}

	public static void ConnectionMysql() throws ClassNotFoundException, SQLException, IOException, JSONException, ParseException {
		Class.forName("com.mysql.jdbc.Driver");
		Connection mysqlcon = DriverManager.getConnection("jdbc:mysql://192.168.0.103:3306/dbtest", "root",
				"MyNewPass");
		String sql = "select * from sbtest1 limit 2";
		stmt = mysqlcon.prepareStatement(sql);
		rs = stmt.executeQuery();
		rsmd = rs.getMetaData();
		int rscount = rsmd.getColumnCount();
		JSONArray jsonarr2 = new JSONArray();
		while (rs.next()) {
			int assign = 0;
			//JSONObject temp = new JSONObject();
LinkedHashMap<String, Object> temp = new LinkedHashMap<String, Object>();
			for (int i = 1; i <= rscount; i++) {
				temp.put(String.valueOf(assign), rs.getObject(i));
				assign++;

			}

			jsonarray.add(temp);

		}
		for (int j = 1; j <= rscount; j++) {
			JSONObject temp1 = new JSONObject();
			temp1.put("name", rsmd.getColumnName(j));
			temp1.put("tablename", "sbtest1");
			jsonarr2.add(temp1);
		}
		json.put("RECORDS", jsonarray);
		json.put("COLUMNS", jsonarr2);
		json.put("aid", "6");
		json.put("tid", "0");
	//	System.out.println(json);
		JSONObject json1 = readJsonFromUrl("http://35.171.234.22:45670/path7");
		System.out.println(json1); 
		flattenJson = JsonFlattener.flattenAsMap(json1.toString());
		//parsing the json to Document
		Document doc = Document.parse(json1.toString());
		ArrayList<Document> columns = (ArrayList<Document>) doc.get("COLUMNS");
		String columnames ="";
		for(int i=0;i<columns.size(); i++){
			Document doc2 = columns.get(i);
			columnames += doc2.get("name")+",";
		}
		columnames =columnames.substring(0, columnames.length()-1);
	    FileWriter fw=new FileWriter("/home/exa1/Desktop/testout.txt");    
        fw.write(columnames+"\n");    
        
		
		ArrayList<Document> records =(ArrayList<Document>) doc.get("RECORDS");
		for(int i=0;i<records.size(); i++){
			Document doc2 = records.get(i);
			Set<String> keys = doc2.keySet();
			String val ="";
				 ArrayList<Integer> set=new ArrayList<Integer>();  
			 for(String s: keys)
				 set.add(Integer.parseInt(s));
			Collections.sort(set);
			for(int keyset : set)
				val +=doc2.get(String.valueOf(keyset))+",";
			
			 val = val.substring(0, val.length()-1);
			 fw.write(val+"\n");
		}
		fw.close();
/*		JSONParser parse = new JSONParser();
		JSONObject jobj = (JSONObject)parse.parse(json1.toString()); 
		JSONArray jsonarr_1 = (JSONArray) jobj.get("COLUMNS");
		for(int i=0; i<jsonarr_1.size(); i++){
			JSONObject jsonobj_1 = (JSONObject)jsonarr_1.get(i);
			System.out.println(jsonobj_1.get("name"));
			
		}*/
	}
	public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
		//InputStream is = new URL(url).openStream();
		URL object = new URL(url);
		HttpURLConnection con = (HttpURLConnection) object.openConnection();
		try {
			
			con.setDoOutput(true);
			con.setDoInput(true);
			con.setRequestProperty("Content-Type", "application/json");
			con.setRequestProperty("Accept", "application/json");
			con.setRequestMethod("POST");
			 OutputStream os = con.getOutputStream();
	            os.write(json.toString().getBytes());
	            os.flush();
			BufferedReader rd = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String jsonText = readAll(rd);
			JSONObject json = new JSONObject(jsonText);
			return json;
		} finally {
			con.disconnect();
		}
	}
	private static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}
}
