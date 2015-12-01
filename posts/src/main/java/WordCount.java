import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public final class WordCount {
	private static final Pattern SPACE = Pattern.compile("/>");
	private static final Pattern attributes = Pattern.compile("(?:OwnerUserId)=\"(-?\\d*)\"");

	public static void main(String[] args) throws Exception {
		JavaSparkContext ctx = new JavaSparkContext("yarn-client", "WordCount", "/usr/lib/spark/", new String[]{"target/WordCount-1.0.jar"});
		JavaRDD<String> file = ctx.textFile("hdfs://hadoop.eecs.qmul.ac.uk:54310//data/stackoverflow/Posts", 1).flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});

		JavaPairRDD<String, String> posts = file.mapToPair(new PairFunction<String, String, String>() {	
			@Override
			public Tuple2<String, String> call(String s) {	
				if(s.contains("OwnerUserId=")){
				        Matcher matcher = attributes.matcher(s);
					List<String> listMatches = new ArrayList<String>();

					while(matcher.find()) { 
						listMatches.add(matcher.group(1));
					}
					return new Tuple2<String, String>("UserId=\"" + listMatches.get(0) + "\"", "PostCount=\"1\"");
				}
				return new Tuple2<String, String>("AccountId Missing", "1");
			}
		});

		posts = posts.reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String a, String b) {
				int i = Integer.parseInt(a.replaceAll("[a-zA-Z\\W]", ""));
				int j = Integer.parseInt(b.replaceAll("[a-zA-Z\\W]", ""));
				return "PostCount=\"" + String.valueOf(i + j) + "\""; 					
			}
		});
		
		System.out.println(posts.takeSample(false, 100, 33));

		
		posts.saveAsTextFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/user/aa327/postcount"); //save on hdfs to folder named sparkcount in user dir "ec09075" <-- MUST BE CHANGED
		ctx.stop();
	}
}

