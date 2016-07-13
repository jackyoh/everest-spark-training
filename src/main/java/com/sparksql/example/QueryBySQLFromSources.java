/**
 * 
 */
package com.sparksql.example;

import java.net.URL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * @author allen
 *
 */
public class QueryBySQLFromSources {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Query By DataFrames From RDD Demo").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		URL resource = QueryByDataFramesFromRDD.class.getResource("/employee.json");
		String filePath = resource.getFile();
		
		// In the next, we start to use SQLContext to interact with Spark SQL
		SQLContext sqlCtx = new SQLContext(sc);
		
		// SQLContext.read() to read data source and convert as DataFrames
		DataFrame employeeDF = sqlCtx.read().json(filePath);
		
		// Register DataFrames as a table
		sqlCtx.registerDataFrameAsTable(employeeDF, "employee");
		
		DataFrame resultDF = sqlCtx.sql("select name, age from employee where age > 30");
		resultDF.show();
	}
}
