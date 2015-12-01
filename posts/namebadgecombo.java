
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class WordCount {
  private static final Pattern SPACE = Pattern.compile(">");

  public static void main(String[] args) throws Exception {
    

    JavaSparkContext ctx = new JavaSparkContext("yarn-client", "WordCount", "/usr/lib/spark/", new String[]{"target/WordCount-1.0.jar"});
    JavaRDD<String> lines = ctx.textFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/data/stackoverflow/Badges", 1);
    JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String s) {
        return Arrays.asList(SPACE.split(s));
      }
    });
    JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
      @Override
      public Tuple2<String, Integer> call(String s) {
	if(s.contains("Name"))
        return new Tuple2<String, Integer>(s.substring(s.indexOf("UserId"), s.indexOf("Date")), 1);
	else return new Tuple2<String, Integer>("0nope", 1);
      }
    });
    JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
      @Override
      public Integer call(Integer i1, Integer i2) {
        return i1 + i2;
      }
    });

     //counts.saveAsTextFile("count");  //save locally to folder named count
     counts.saveAsTextFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/user/jv300/sparkcount"); //save on hdfs to folder named sparkcount in user dir "ec09075" <-- MUST BE CHANGED
    ctx.stop();
  }
}
