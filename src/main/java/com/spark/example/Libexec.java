/**
 * 
 */
package com.spark.example;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * @author allen
 *
 */
public class Libexec {

	/**
	 * 
	 */
	public Libexec() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException {
		SparkConf conf = new SparkConf().setAppName("Word_Count").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		String path = WordCount.class.getResource("/libexec.dat").getPath();
		
		JavaRDD<String> input = sc.textFile(path);
		
		JavaRDD<String> dirResult = input.filter(new Function<String, Boolean>(){

			@Override
			public Boolean call(String line) throws Exception {
				return line.startsWith("d");
			}
			
		});
		
		JavaRDD<String> dirAndDateResult = dirResult.filter(new Function<String, Boolean>(){
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			Date base = df.parse("2015-06-01");
			@Override
			public Boolean call(String line) throws Exception {
				String[] words = line.split(" ");
				boolean result = false;
				for (String word: words) {
					try { 
						Date date = df.parse(word);
						result = date.after(base);
						break;
					} catch(ParseException pe) {}
				}
				return result;
			}
			
		});
		
		System.out.println(dirResult.count());
		System.out.println(dirAndDateResult.count());
		
		sc.close();
	}

}
