package com.sparksql.example;

import java.io.Serializable;
import java.net.URL;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class GetResultFromDataFrames {
	public static class Employee implements Serializable{
		private static final long serialVersionUID = 1L;
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
		
		DataFrame resultDF = sqlCtx.sql("select id, name, age from employee where age > 30");
		
		/**
		 * How to get result from DataFrames?
		 * 
		 * **/
		
		List<Employee> result = resultDF.javaRDD().map(new Function<Row, Employee>(){

			@Override
			public Employee call(Row row) throws Exception {
				Employee employee = new Employee();
				employee.setId(row.getString(0));
				employee.setName(row.getString(1));
				employee.setAge(Integer.parseInt(row.getString(2)));
				return employee;
			}
			
		}).collect();
		
		System.out.println("==== Find employee who's age are greater than 30 ====");
		for(Employee employee: result){
			System.out.println(employee.getName() + ", age= " + employee.getAge());
		}
		
	}

}
