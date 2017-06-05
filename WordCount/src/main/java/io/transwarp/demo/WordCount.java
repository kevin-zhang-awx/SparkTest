package io.transwarp.demo;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by zxh on 2017/5/6.
 */
public class WordCount implements Serializable{
    private transient SparkConf conf;
    private transient JavaSparkContext sparkContext;
    private int partitions;
    private String file;

    class Printer implements VoidFunction<Tuple2<String, Integer>> {
        public void call(Tuple2<String, Integer> tuple2) {
            System.out.println(tuple2._1() + "-->" + tuple2._2());
        }
    }


    public WordCount(int partitions, String file) {
        conf = new SparkConf().setAppName("wordCount").setMaster("local");
        sparkContext = new JavaSparkContext(conf);
        this.partitions = partitions;
        this.file = file;
    }


    public void count() {
        JavaRDD<String> lines = sparkContext.textFile(file, partitions);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) {
               return Arrays.asList(s.split(" ")).iterator();
           }
        });

        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) {
                return new Tuple2<String, Integer>(word, 1);
            }
        });



        //reduce
        JavaPairRDD<String, Integer> result = counts.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer num1, Integer num2) {
                return num1 + num2;
            }
        });

        result.foreach(new Printer());

        int totalCount = result.values().reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer cnt1, Integer cnt2) throws Exception {
                return cnt1 + cnt2;
            }
        });

        System.out.println("totalCount:"+totalCount);
    }


    public static void main(String[] args) {
        int partitions = Integer.parseInt(args[0]);
        String file = args[1];

        WordCount wordCount = new WordCount(partitions, file);
        wordCount.count();
    }
}



