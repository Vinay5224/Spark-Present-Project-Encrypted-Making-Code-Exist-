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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.exf.spark.ReadingFile;
import com.github.wnameless.json.flattener.JsonFlattener;

public class exm {

	static PreparedStatement stmt = null;
	static ResultSet rs = null;
	static ResultSetMetaData rsmd = null;
	static JSONObject json = new JSONObject();
	static JSONArray jsonarray = new JSONArray();
	static Map<String, Object> flattenJson;
	public static  Dataset<Row> sentenceDataFrame2; 
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, JSONException, ParseException {
		// TODO Auto-generated method stub
		ConnectionMysql();

	}

	@SuppressWarnings("null")
	public static void ConnectionMysql() throws ClassNotFoundException, SQLException, IOException, JSONException, ParseException {
		//Class.forName("com.mysql.jdbc.Driver");
		Class.forName("org.apache.hive.jdbc.HiveDriver");
	//	Connection mysqlcon = DriverManager.getConnection("jdbc:mysql://targetschemas.cqzcaawwqsmw.us-east-1.rds.amazonaws.com:3306/individual_src", "secured",
			//	"pwd4secured");
		Connection mysqlcon = DriverManager.getConnection("jdbc:hive2://192.168.0.184:10000/individual_profile_schema_src","","");
		String sql = "select email_address,first_name,last_name,gender,birthday from individual_profile_schema_src.individual_profile_schema_src"; //limit 10";
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
			temp1.put("tablename", "individual_profile_schema");
			jsonarr2.add(temp1);
		}
		json.put("RECORDS", jsonarray);
		json.put("COLUMNS", jsonarr2);
		json.put("aid", "5");
		json.put("tid", "0");
	//	System.out.println(json);
		JSONObject json1 = readJsonFromUrl("http://localhost:45670/path7");
		System.out.println(json1); 
		//flattenJson = JsonFlattener.flattenAsMap(json1.toString());
		//parsing the json to Document
		Document doc = Document.parse(json1.toString());
		ArrayList<Document> columns = (ArrayList<Document>) doc.get("COLUMNS");
		//String columnames ="";
		List<String> columnames = new ArrayList<String>();
		for(int i=0;i<columns.size(); i++){
			Document doc2 = columns.get(i);
			//columnames += doc2.get("name")+",";
			columnames.add(doc2.get("name").toString());
		}
		//columnames =columnames.substring(0, columnames.length()-1);
	   // FileWriter fw=new FileWriter("/home/exa1/Desktop/testout.txt");    
        //fw.write(columnames+"\n");    
        List<Row> data2 =new ArrayList<Row>();
        List<Row> arrfulldata = new ArrayList<Row>();
       
		
		ArrayList<Document> records =(ArrayList<Document>) doc.get("RECORDS");
		for(int i=0;i<records.size(); i++){
			Document doc2 = records.get(i);
			Set<String> keys = doc2.keySet();
			String val ="";
			//List<String> val = new ArrayList<String>();
				 ArrayList<Integer> set=new ArrayList<Integer>();  
			 for(String s: keys)
				 set.add(Integer.parseInt(s));
			Collections.sort(set);
			for(int keyset : set)
				val +=doc2.get(String.valueOf(keyset))+",";
			//	val.add(doc2.get(keyset).toString());
			//	String[] str = val.split(",");
			// val = val.substring(0, val.length()-1);
	//		 Row row = RowFactory.create(Arrays.asList(val.split(",")));
			// Row row = RowFactory.create(Arrays.asList(val.split(",")));
			// data2.add(RowFactory.create(str));
		//data2.add(RowFactory.create(Arrays.asList(val.split(","))));
			 data2 = Arrays.asList(
				        RowFactory.create(val.split(",")));
			 arrfulldata.addAll(data2);
			 //fw.write(val+"\n");
			//data2.add(RowFactory.create(row));
		}
		//fw.close();
		/*  StructType schema2 = new StructType(new StructField[] {new StructField("email_address", DataTypes.StringType, false,Metadata.empty()),
			        new StructField("first_name", DataTypes.StringType, false,Metadata.empty()),new StructField("last_name", DataTypes.StringType, false,Metadata.empty())
			       , new StructField("gender", DataTypes.StringType, false,Metadata.empty())});*/
		StructType schema = createSchema(columnames);
		  SparkSession spark = SparkSession.builder().master("local").appName("Reading spark files").getOrCreate();
			        sentenceDataFrame2 = spark.createDataFrame(arrfulldata, schema);
			        ReadingFile rdf =new ReadingFile();
			        rdf.hadoop(sentenceDataFrame2);
			       // sentenceDataFrame2.show();
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
	   public static StructType createSchema(List<String> tableColumns){

	        List<StructField> fields  = new ArrayList<StructField>();
	        for(String column : tableColumns){         

	                fields.add(DataTypes.createStructField(column, DataTypes.StringType, true));            

	        }
	        return DataTypes.createStructType(fields);
	    }
}
