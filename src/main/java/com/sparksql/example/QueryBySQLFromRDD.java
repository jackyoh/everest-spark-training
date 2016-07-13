/**
 * 
 */
package com.sparksql.example;

import java.net.URL;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import com.sparksql.example.QueryByDataFramesFromRDD.Employee;

/**
 * @author allen
 *
 */
public class QueryBySQLFromRDD {
	public static class Employee {
		private String id;
		private String name;
		private int age;
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public int getAge() {
			return age;
		}
		public void setAge(int age) {
			this.age = age;
		}
		
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Query By SQL From RDD Demo").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		URL resource = QueryByDataFramesFromRDD.class.getResource("/employee.txt");
		String filePath = resource.getFile();
		
		// Read a text file
		JavaRDD<String> jsonFile = sc.textFile(filePath);
		
		// Convert the data to Employee JavaBeans
		JavaRDD<Employee> employeeRdd = jsonFile.map(new Function<String, Employee>(){

			@Override
			public Employee call(String line) throws Exception {
				String[] splitted = line.split(",");
				Employee employee = new Employee();
				employee.setId(splitted[0]);
				employee.setName(splitted[1]);
				employee.setAge(Integer.parseInt(splitted[2]));
				return employee;
			}
		});
		
		
		// In the next, we start to use SQLContext to interact with Spark SQL
		SQLContext sqlCtx = new SQLContext(sc);
		
		// Convert RDD to DataFrames
		DataFrame employeeDF = sqlCtx.createDataFrame(employeeRdd, Employee.class);
		
		// Register DataFrames as a table
		sqlCtx.registerDataFrameAsTable(employeeDF, "employee");
		
		// Use SQL to query that table
		DataFrame resultDF = sqlCtx.sql("select name, age from employee where age > 30");
		resultDF.show();
	}

}
