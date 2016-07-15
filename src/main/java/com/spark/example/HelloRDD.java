/**
 * 
 */
package com.spark.example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * @author allen
 *
 */
public class HelloRDD {

	/**
	 * 
	 */
	public HelloRDD() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Hello_World").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		
		JavaRDD<Integer> list = sc.parallelize(data);
		
		int result = list.reduce(new Function2<Integer, Integer, Integer>(){

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
		
		System.out.println(result);
		
		sc.close();
	}

}
