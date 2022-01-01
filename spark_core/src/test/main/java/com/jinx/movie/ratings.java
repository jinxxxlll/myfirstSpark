package com.jinx.movie;

import com.jinx.model.movieSort;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.util.List;

/**
 * usr: usrId + FM + age + occ + post
 * movie: movieId + name + type
 * occ: id + name
 * rating: usrId + movieId +ration + time
 *
 */
public class ratings implements Serializable {

    public static void main(String[] args) {
        String path="D:\\Gdisk\\spark\\spark_v1\\log\\";

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("sort").setMaster("local"));
        new ratings().rating_sort(sc,path);
    }

    /*
    基于Serializable，Ordered 实现二次排序
     */
    public void rating_sort( JavaSparkContext sc,String path){
        JavaRDD<String> JavaRDD = sc.textFile(path + "ratings.log");
        JavaPairRDD javaPairRDD=JavaRDD.mapToPair(new PairFunction<String, movieSort, String>() {
            private static final long serialVersionUid=1L;
            @Override
            public Tuple2<movieSort, String> call(String s) throws Exception {
                String[] words = s.split("::");
                movieSort movieSort = new movieSort(Integer.parseInt(words[2]), Integer.parseInt(words[0]));
                return  new Tuple2<movieSort,String>(movieSort,words[1]);
            }
        });
        JavaPairRDD res = javaPairRDD.sortByKey(false);
        List<Tuple2<movieSort,String>> take = res.take(10);
        org.apache.spark.api.java.JavaRDD<Tuple2<movieSort, String>> parallelize = sc.parallelize(take);
        for (Tuple2<movieSort, String> tuple2 : parallelize.collect()) {
            System.out.println(tuple2);
        }
        /* 二次取值
        JavaRDD rdd=res.map(new Function<Tuple2<movieSort, String>,String>() {
            private static final long serialVersionUid=1L;
            @Override
            public String call(Tuple2<movieSort,String> o) throws Exception {
                return o.toString();
            }

        });
        List<Tuple2<movieSort,String>> take = res.take(10);
        for (Tuple2<movieSort,String> o : take) {
            System.out.println(o);
        }
        */

    }


}
