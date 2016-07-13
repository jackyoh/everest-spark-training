package com.sparksql.example;

import java.net.URL;
import java.util.LinkedHashMap;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

public class HelloDataFramesAPI {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Query By DataFrames From RDD Demo").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		URL resource = QueryByDataFramesFromRDD.class.getResource("/employee.json");
		String filePath = resource.getFile();
		
		// In the next, we start to use SQLContext to interact with Spark SQL
		SQLContext sqlCtx = new SQLContext(sc);
		
		// SQLContext.read() to read data source and convert as DataFrames
		DataFrame employeeDF = sqlCtx.read().json(filePath);
		
		/**
		 * DataFrames show demo
		 * It's most useful function to display the content of DataFrames
		 * 
		 * **/
		employeeDF.show();
		
		/** 
		 * DataFrames select demo
		 * The following are equivalent:
		 * 
		 * Notes: The first one is a short hand expression, but
		 * I recommend you use the second expression is more better.
		 * 
		 * Use df.col(YOUR_COLUMN_NAME), Spark return a org.apache.spark.sql.Column instance
		 * you can use all the function of Column in following link
		 * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column
		 * 
		 * **/
		employeeDF.select("age").show();
		employeeDF.select(employeeDF.col("age")).show();
		
		/**
		 * It's a good example for why to use org.apache.spark.sql.Column
		 * 
		 * **/
		employeeDF.select(employeeDF.col("age").plus(1)).show();
		
		
		/**
		 * DataFrames filter demo
		 * It's also a good example for using org.apache.spark.sql.Column
		 *  
		 *  **/
		employeeDF.filter(employeeDF.col("age").gt(30)).show();
		employeeDF.filter(employeeDF.col("age").gt(30).and(employeeDF.col("age").lt(35))).show();
		
		
		/**
		 * DataFrames aggregate demo
		 * 
		 * **/
		
		employeeDF.agg(new LinkedHashMap<String, String>(){{put("age", "max");}}).show();
	}

}
