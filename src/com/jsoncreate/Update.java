package com.jsoncreate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.simple.JSONArray;

import com.exf.spark.ReadingFile;
import com.exf.spark.ReadingTxtFile;

public class Update {
static JSONObject jsontxtarr = new JSONObject();
static Dataset<Row> txtrowencrypt;
	public static void main(String[] args) throws IOException, JSONException, ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		
		String columnames2encrypt = "first_name,last_name,gender,birthday,email_address";
		JSONArray arrtxt = new JSONArray();
		String[] splittingcol = columnames2encrypt.split(",");
		for(int i=0;i<splittingcol.length;i++){
			JSONObject temp1 = new JSONObject();
			temp1.put("name", splittingcol[i]);
			temp1.put("tablename", "individual_profile_schema");
			arrtxt.add(temp1);
		}
		BufferedReader br = new BufferedReader(new FileReader(new File("/home/exa1/Desktop/individual_profile_schema.txt")));
		  // this will read the first line
		//column names		
		 String line1=null;
		 List<Integer> index =new ArrayList<Integer>();
		 while((line1=br.readLine())!=null){
			 String[] colsplit =line1.split(",");
			 for(int i=0;i<colsplit.length;i++){
				for(int j=0;j<splittingcol.length;j++){
					if(splittingcol[j].equalsIgnoreCase(colsplit[i])){
						index.add(i);
					}
							
				}
			 }
			 break;
		 }
	JSONArray jsonrows = new JSONArray();
		//Taking particular columns from the text file 
		 while ((line1 = br.readLine()) != null){
		  	String[] splitrow = line1.split(",");
		  	int assign = 0;
		  	JSONObject temp = new JSONObject();
		  	for (int i : index) {
		  			temp.put(String.valueOf(assign), splitrow[i]);
		  			assign++;
		  	 }
		  	jsonrows.add(temp);
		 }
		 //preparing path7 api call
		 jsontxtarr.put("RECORDS", jsonrows);
		 jsontxtarr.put("COLUMNS", arrtxt);
		 jsontxtarr.put("aid", "5");
		 jsontxtarr.put("tid", "0");
System.out.println(jsontxtarr.toString()+"\n"+index.size());
		 JSONObject outputjson = readJsonFromUrl("http://localhost:45670/path7");
			Document doc = Document.parse(outputjson.toString());
			ArrayList<Document> columns = (ArrayList<Document>) doc.get("COLUMNS");
			List<String> columnames = new ArrayList<String>();
			for(int i=0;i<columns.size(); i++){
				Document doc2 = columns.get(i);
				columnames.add(doc2.get("name").toString());
			}
			
		      List<Row> data2 =new ArrayList<Row>();
		        List<Row> arrfulldata = new ArrayList<Row>();
		       
				
				ArrayList<Document> records =(ArrayList<Document>) doc.get("RECORDS");
				for(int i=0;i<records.size(); i++){
					Document doc2 = records.get(i);
					Set<String> keys = doc2.keySet();
					String val ="";
					for(String keyset : keys)
						val +=doc2.get(keyset)+",";
					 val = val.substring(0, val.length()-1);
					 data2 = Arrays.asList(
						        RowFactory.create(val.split(",")));
					 arrfulldata.addAll(data2);
				}
				StructType schema = createSchema(columnames);
				  SparkSession spark = SparkSession.builder().master("local").appName("Reading spark files").getOrCreate();
				  txtrowencrypt = spark.createDataFrame(arrfulldata, schema);
					        ReadingTxtFile rdtxt =new ReadingTxtFile();
					        rdtxt.hadoop(txtrowencrypt);
		 
		 
	}
	public static JSONObject readJsonFromUrl(String url) throws IOException, JSONException {
		//InputStream is = new URL(url).openStream();
		URL object = new URL(url);
		HttpURLConnection contxt = (HttpURLConnection) object.openConnection();
		try {
			
			contxt.setDoOutput(true);
			contxt.setDoInput(true);
			contxt.setRequestProperty("Content-Type", "application/json");
			contxt.setRequestProperty("Accept", "application/json");
			contxt.setRequestMethod("POST");
			 OutputStream os = contxt.getOutputStream();
	            os.write(jsontxtarr.toString().getBytes());
	            os.flush();
			BufferedReader rd = new BufferedReader(new InputStreamReader(contxt.getInputStream()));
			
			String jsonText = readAll(rd);
			JSONObject json = new JSONObject(jsonText);
			return json;
		} finally {
			contxt.disconnect();
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
