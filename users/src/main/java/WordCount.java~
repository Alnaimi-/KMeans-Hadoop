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
	private static final Pattern attributes = Pattern.compile("((?:\\bId\\b|Rep|Up|Down)\\w*=\"(-?\\d*)\")");

	public static void main(String[] args) throws Exception {
		JavaSparkContext ctx = new JavaSparkContext("yarn-client", "WordCount", "/usr/lib/spark/", new String[]{"target/WordCount-1.0.jar"});
		JavaRDD<String> file = ctx.textFile("hdfs://hadoop.eecs.qmul.ac.uk:54310//data/stackoverflow/Users", 1).flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});

		JavaPairRDD<String, String> users = file.mapToPair(new PairFunction<String, String, String>() {	
			@Override
			public Tuple2<String, String> call(String s) {	
				if(s.contains("Id=")){
				        Matcher matcher = attributes.matcher(s);
					List<String> listMatches1 = new ArrayList<String>(), listMatches2 = new ArrayList<String>();

					while(matcher.find()) { 
						listMatches1.add(matcher.group(1));
						listMatches2.add(matcher.group(2));
					}
					String values = listMatches1.get(1) + " " + listMatches1.get(2) + " " + listMatches1.get(3);
					return new Tuple2<String, String>("UserId=\"" + listMatches2.get(0) + "\"", values);
				}
				return new Tuple2<String, String>("AccountId Missing", "1");
			}
		});

		file = ctx.textFile("hdfs://hadoop.eecs.qmul.ac.uk:54310//user/aa327/sparkcount", 1); // read the badge count output 

		JavaPairRDD<String, String> badges = file.mapToPair(new PairFunction<String, String, String>() { // map pair the badge output	
			@Override
			public Tuple2<String, String> call(String s) {
				String[] split = s.split(",");				
				return new Tuple2<String, String>(split[0].substring(1), split[1].substring(0, split[1].length()-1));
			}
		});

		JavaPairRDD<String, Tuple2<String, String>> joined = users.join(badges); // Joins on common keys, returning the key as String and a Tuple2 representing values of joined RDD's

		file = ctx.textFile("hdfs://hadoop.eecs.qmul.ac.uk:54310//user/aa327/postcount", 1);

		JavaPairRDD<String, String> posts = file.mapToPair(new PairFunction<String, String, String>() { // map pair the badge output	
			@Override
			public Tuple2<String, String> call(String s) {
				String[] split = s.split(",");				
				return new Tuple2<String, String>(split[0].substring(1), split[1].substring(0, split[1].length()-1));
			}
		});

		joined.saveAsTextFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/user/aa327/joincount");
		
		file = ctx.textFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/user/aa327/joincount",1);		
		users = file.mapToPair(new PairFunction<String, String, String>() {	
			@Override
			public Tuple2<String, String> call(String s) {
				String[] split = s.split(",");
				String values = split[1] + " " + split[2];
				return new Tuple2<String, String>(split[0].substring(1), values.substring(0, values.length()-2));
			}
		});
		
		joined = users.join(posts);

		joined.saveAsTextFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/user/aa327/joinpost");
	
		file = ctx.textFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/user/aa327/joinpost");
		users = file.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) {
				String[] split = s.split(",");
				int postsCount = Integer.parseInt(split[2].replaceAll("[a-zA-Z]*\\W", ""));
				int rep = Integer.parseInt(split[1].replaceAll("[a-zA-Z]*[^\\s\\d]", "").split(" ")[0]);
				int avg = rep/postsCount;
				String newValues = split[1].substring(2) + " " + split[2].substring(0, split[2].length()-2) + " PostRatio=\"" + String.valueOf(avg) + "\"";
				return new Tuple2<String, String>(split[0].substring(1), newValues);
				
			}
		});		
	
		System.out.println(users.takeSample(false, 100, 33));
		users.saveAsTextFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/user/aa327/finaljoindata");
		ctx.stop();
	}
}

