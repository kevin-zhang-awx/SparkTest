package io.transwarp.aggregatetest;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.math.BigDecimal;
import java.util.List;

/**
 * Created by zxh on 2017/5/6.
 */
public class Aggregate {
    public static void main(final String[] args) {

        SparkConf conf = new SparkConf().setAppName("Aggregate").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //获取文件RDD
        String files = args[0];
        int splits = Integer.parseInt(args[1]);
        JavaRDD<String> data = sc.textFile(files, splits);


        //聚合字段的pairRDD
        JavaPairRDD<Integer, BigDecimal> groupby = data.mapToPair(new PairFunction<String, Integer, BigDecimal>() {
            public Tuple2<Integer, BigDecimal> call(String s) throws Exception {
                String[] strings = s.split("\\|");
                int customerkey = Integer.parseInt(strings[1]);
                BigDecimal totalprice = new BigDecimal(strings[3]);
                return new Tuple2<Integer, BigDecimal>(customerkey, totalprice);
            }
        });


        //reduce
        JavaPairRDD<Integer, BigDecimal> result =
                groupby.reduceByKey(new Function2<BigDecimal, BigDecimal, BigDecimal>() {
            public BigDecimal call(BigDecimal num1, BigDecimal num2) throws Exception {
                return num1.add(num2);
            }
        });

        result.sortByKey(true);

        int size = 10;
        int i=0;
        List<Tuple2<Integer, BigDecimal>> resultList = result.collect();
        for(Tuple2<Integer, BigDecimal> t:resultList){
            if(i++<size)
                System.out.println(t._1+","+t._2);
            else
                break;
        }
    }
}
