import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.Iterator;

public final class BadgeCount {
	private static final Pattern GREATERTHAN = Pattern.compile(">");
	private enum BadgeType {
		QUESTION,
		ANSWER
	}

	public static void main(String[] args) throws Exception {
		JavaSparkContext ctx = new JavaSparkContext("yarn-client", "BadgeCount", "/usr/lib/spark/", new String[]{"target/BadgeCount-1.0.jar"});

		JavaRDD<String> file = ctx.textFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/user/aa327/BadgesScore.xml", 1);    
		final ArrayList<String> badges = new ArrayList<String>(file.collect());

		file = ctx.textFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/data/stackoverflow/Badges", 1).flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(GREATERTHAN.split(s));
			}
		});

		JavaPairRDD<String, String> values = file.mapToPair(new PairFunction<String, String, String>() {	
			@Override
			public Tuple2<String, String> call(String s) {	
				if(s.contains("Name")){
					int nameIndex = s.indexOf("Name=");
					int dateIndex = s.indexOf("Date=");
					int userIdIndex = s.indexOf("UserId=");
					String userId = s.substring( (userIdIndex + 8), (nameIndex - 2) ).trim();

					String badgeName = s.substring( (nameIndex + 6), (dateIndex - 2) ); // badge from the actual user row	

					int level = -1;					
					BadgeType badgeType = BadgeType.QUESTION;

					// Find type of badge
					for(String row : badges) {
						String checkBadgeName = row.substring(row.indexOf("Name=") + 6, row.indexOf("Type=") - 2);

						if(badgeName.equals(checkBadgeName)){
							String typeString = row.substring(row.indexOf("Type=") + 6, row.indexOf("Colour=") - 2);
							level = Integer.parseInt(row.substring(row.indexOf("Colour=") + 8, row.indexOf(">") - 1));
							badgeType = typeString.equals("0") ? BadgeType.QUESTION : BadgeType.ANSWER;
						}
					}
					
					// If badge is worth nothing, return base case 
					if(level==-1){
						return new Tuple2<String, String>("UserId=BASECASE", "Questions={Bronze=0 Silver=0 Gold=0} Answers={Bronze=0 Silver=0 Gold=0}"); // fail-safe dummy statement
					}

					int[] levels = {0,0,0,0,0,0};
					for(int i=0; i<3; i++){
						if(i==level-1){
							if(badgeType == BadgeType.QUESTION){
								i+=3;
							}
							levels[i]++;
							break;
						}
					}

					return new Tuple2<String, String>("UserId=\""+userId+"\"", "Questions={Bronze=" + levels[0] + " Silver=" + levels[1] + " Gold=" + levels[2] + "} Answers={Bronze=" + levels[3] + " Silver=" + levels[4] + " Gold=" + levels[5] +"}");	

				}
				return new Tuple2<String, String>("UserId=BASECASE", "Questions={Bronze=0 Silver=0 Gold=0} Answers={Bronze=0 Silver=0 Gold=0}"); // fail-safe dummy statement
			}
		});

		JavaPairRDD<String, String> counts = values.reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String a, String b) {
				int[] levels = new int[6];
				
				a = a.replaceAll("[^\\d ]","");
				b = b.replaceAll("[^\\d ]","");

				String[] aSplit = a.split(" ");
				String[] bSplit = b.split(" ");
				for(int i = 0; i < levels.length; i++){
					levels[i] = Integer.parseInt(aSplit[i]) + Integer.parseInt(bSplit[i]);
				}

				return "Questions={Bronze=\"" + levels[0] + "\" Silver=\"" + levels[1] + "\" Gold=\"" + levels[2] + "\"} Answers={Bronze=\"" + levels[3] + "\" Silver=\"" + levels[4] + "\" Gold=\"" + levels[5] +"\"}";
			}
		});

		counts.saveAsTextFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/user/aa327/sparkcount");
		ctx.stop();
	}
}
