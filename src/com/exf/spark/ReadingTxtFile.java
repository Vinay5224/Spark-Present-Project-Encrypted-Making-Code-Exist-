package com.exf.spark;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Range;
import com.jsoncreate.exm;

public class ReadingTxtFile {

	public static void hadoop(Dataset<Row> secondset) throws IOException, ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		SparkSession spark = SparkSession.builder().master("local").appName("Reading spark files").getOrCreate();
		
		//Dataset<Row> encryptcol = spark.read().format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").load("/home/exa1/Desktop/testout.txt").na().fill("");
		//encryptcol.show();
		Properties prop = new Properties();
		prop.put("user", "secured");
		prop.put("password", "pwd4secured");
		//Dataset<Row> plaincol = spark.read().jdbc("jdbc:mysql://targetschemas.cqzcaawwqsmw.us-east-1.rds.amazonaws.com:3306/individual_src", "individual_profile_schema", prop).limit(10);
		Dataset<Row> plaincol = spark.read().format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").load("/home/exa1/Desktop/individual_profile_schema.txt").na().fill("");
		plaincol.show(2);
		plaincol.createOrReplaceTempView("SBTEST1");
		String[] col = plaincol.columns();
		System.out.println(col.length);
		Dataset<Row> firstset = spark.sql("SELECT master_individual_id,swid,customer_id,marketing_effort_key,template_key,title,middle_initial,geography_key,age,facebook_connect_flg,employee_flg,registration_date,registration_affiliate_name,marketing_effort_code_key,registration_flg,affluence,affluent_suburbia,numzip,tenure,postal_code,country,state_province,state_abbreviation,city,dma_code,dma_name,msa_name,age_range,ethnic_code,ethnic_group,ethnic_group_cd,household_income,aerobic_exercise_ind,assimilation_cd,cable_tv_ind,child_age_0to2_present,child_age_3to5_present,child_age_6to10_present,child_age_11to15_present,child_age_16to17_present,card_holder,gas_department_retail_card_holder,travel_entertainment_card_holder,premium_card_holder,dwelling_type_cd,first_individual_age_range_cd,first_individual_education_cd,first_individual_gender_cd,first_individual_occ_cd,home_assessed_value_num,home_equity_available_cd,home_market_value_cd,home_owner_ind,own_rent,household_size_cd,international_travel_ind,investing_grouping_ind,length_of_residence_cd,likely_investors_ind,marital_status,net_worth_cd,number_of_adults,number_of_children,personal_investments_ind,children_present,running_exercise_ind,satellite_dish_tv_ind,second_individual_age_range_cd,second_individual_education_cd,second_individual_gender_cd,stocks_bonds_investment_ind,vacation_travel_rv_ind"
 + " FROM SBTEST1");
		firstset = firstset.withColumn("id", functions.row_number().over(Window.orderBy("swid")));
		//Dataset<Row> secondset = spark.sql("SELECT last_name,birthday FROM SBTEST1");
	    //secondset = secondset.withColumn("id", functions.row_number().over(Window.orderBy("last_name")));
		Dataset<Row> second = secondset;
		second = second.withColumn("id", functions.row_number().over(Window.orderBy("first_name")));
		
	//	Dataset<Row> finalset = firstset.join(secondset, firstset.col("id").equalTo(secondset.col("id")), "outer").drop("id");
	Dataset<Row> finalset = firstset.join(second, firstset.col("id").equalTo(second.col("id")), "outer").drop("id");	
	finalset.show();	
	
		//finalset.write().
		//format("jdbc").
	//	option("url", "jdbc:hive2://192.168.0.184:21050").option("dbtable", "bharath")
		//.option("user","").option("password", "").saveAsTable("test1");
		//jdbc("jdbc:hive2://192.168.0.184:21050/exf_test", "test11", prop1);
		
	//original one
		finalset.write().mode("append").csv("hdfs://192.168.0.184:8020/user/cloudera/individual_profile_schema/");
		//toJavaRDD().saveAsTextFile("hdfs://192.168.0.184:8020/user/cloudera/individual_profile_schema");
		//.format("com.databricks.spark.csv").save("hdfs://192.168.0.184:8020/user/cloudera/individual_profile_schema2");
		//"hdfs://192.168.0.108:8020/user/exa4/SS.csv");
				//Dimjoin.join(dimp, Dimjoin.col("FCT_CLAIM_UNIVERSE_MEMBER_PRIMARY_PROVIDER_ID").equalTo(dimp.col("DIM_PROVIDER_PROVIDER_ID")), "inner");

		String allcolumns[] = finalset.columns();
		String insertcol = "";
		for(String s: allcolumns)
			insertcol+=s+" String, ";
		
		insertcol=insertcol.substring(0,insertcol.length()-2);
	//	Hadoop2hive(insertcol);
		System.out.println("Task Completed");
	}
	
/*	
	public static void Hadoop2hive(String str) throws ClassNotFoundException, SQLException{
		
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive2://192.168.0.184:21050/exf_test;auth=noSasl","","");
		Statement stmt = con.createStatement();
		Statement loadstmt = con.createStatement();
		str = "CREATE EXTERNAL TABLE individual_profile_schema ("+str+") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'hdfs://192.168.0.184:8020/user/cloudera/individual_profile_schema';";
		//String sql = "LOAD DATA INPATH 'hdfs://192.168.0.184/user/cloudera/file_1.csv' INTO TABLE TEST8;";
		System.out.println(str);
		stmt.execute(str);
		//loadstmt.execute(sql);
	    con.close();
				String sqlStatement = "select * from tem";
		ResultSet rs = stmt.executeQuery(sqlStatement);
		while(rs.next()){			
			System.out.println(rs.getString(1));
		
	}
	}*/

}


//Adding the columns directly to the dataset
/* 	List<Row> data2 = Arrays.asList(
        RowFactory.create("222-2222-5555", "Tata","1"),
        RowFactory.create("7777-88886","WestSide", "2"),
        RowFactory.create("22222-22224","Reliance", "3"),
        RowFactory.create("33333-3333","V industries", "4"));

        StructType schema2 = new StructType(new StructField[] {new StructField("label2", DataTypes.StringType, false,Metadata.empty()),
        new StructField("sentence2", DataTypes.StringType, false,Metadata.empty()),new StructField("list", DataTypes.StringType, false,Metadata.empty())});

        Dataset<Row> sentenceDataFrame2 = spark.createDataFrame(data2, schema2);
        sentenceDataFrame2.show();*/
