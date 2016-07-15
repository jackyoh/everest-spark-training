package com.spark.example;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {

	public WordCount() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Word_Count").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String path = WordCount.class.getResource("/wordcount.txt").getPath();
		
		JavaRDD<String> input = sc.textFile(path);
		
		JavaRDD<String> word = input.flatMap(new FlatMapFunction<String, String>(){

			@Override
			public Iterable<String> call(String line) throws Exception {
				return Arrays.asList(line.split(" "));
			}
			
		});
		
		JavaPairRDD<String, Integer> word2num = word.mapToPair(new PairFunction<String, String, Integer>(){

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				return new Tuple2<String, Integer>(word, 1);
			}
			
		});
		
		JavaPairRDD<String, Integer> wordcount = word2num.reduceByKey(new Function2<Integer, Integer, Integer>(){

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
			
		});
		
		List<Tuple2<String, Integer>> result = wordcount.collect();
		
		for(Tuple2<String, Integer> tuple: result) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		
		sc.close();
		
	}

}
