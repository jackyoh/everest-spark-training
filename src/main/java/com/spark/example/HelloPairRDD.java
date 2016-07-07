/**
 * 
 */
package com.spark.example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author allen
 *
 */
public class HelloPairRDD {

	/**
	 * 
	 */
	public HelloPairRDD() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Hello_World").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> data = Arrays.asList("Allen", "Bob", "Cindy", "Bob", "Allen", "Fred", "Cindy");
		
		JavaRDD<String> list = sc.parallelize(data);
		
		JavaPairRDD<String, Integer> nameValuePair = list.mapToPair(new PairFunction<String, String, Integer>(){

			@Override
			public Tuple2<String, Integer> call(String t) throws Exception {
				return new Tuple2<String, Integer>(t, 1);
			}
			
		});
		
		JavaPairRDD<String, Integer> name2Count = nameValuePair.reduceByKey(new Function2<Integer, Integer, Integer>(){

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
		
		List<Tuple2<String, Integer>> result = name2Count.collect();
		
		for(Tuple2<String, Integer> tuple: result) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
	}

}
