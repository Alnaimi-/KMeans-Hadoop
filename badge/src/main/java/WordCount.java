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

public final class WordCount {
	private static final Pattern SPACE = Pattern.compile(">");

	public static void main(String[] args) throws Exception {
		JavaSparkContext ctx = new JavaSparkContext("yarn-client", "WordCount", "/usr/lib/spark/", new String[]{"target/WordCount-1.0.jar"});

		JavaRDD<String> file = ctx.textFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/user/aa327/BadgesScore.xml", 1);    
		List<String> tempList = file.collect();
		final ArrayList<String> badges = new ArrayList<String>(tempList);

		file = ctx.textFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/data/stackoverflow/Badges", 1).flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});

		JavaPairRDD<String, String> values = file.mapToPair(new PairFunction<String, String, String>() {	
			@Override
			public Tuple2<String, String> call(String s) {	
				if(s.contains("Name")){
					String badge = s.substring(s.indexOf("Name=") + 6, s.indexOf("Date") - 2); // badge from the actual user row	

					int level = -1;					
					boolean QA = true;

					for(String row : badges) {
						String check = row.substring(row.indexOf("Name=") + 6, row.indexOf("Type=") - 2);

						if(badge.equals(check)){
							String type = row.substring(row.indexOf("Type=") + 6, row.indexOf("Colour=") - 2);
							level = Integer.parseInt(row.substring(row.indexOf("Colour=") + 8, row.indexOf(">") - 1));
							QA = !type.equals("0");
						}
					}

					if (QA){
						if (level == 1)  
							return new Tuple2<String, String>(s.substring(s.indexOf("UserId"), s.indexOf("Name") -1).trim(), "Questions Bronze 0 Questions Silver 0 Questions Gold 0 Answers Bronze 1 Answers Silver 0 Answers Gold 0");
						else if (level == 2)  
							return new Tuple2<String, String>(s.substring(s.indexOf("UserId"), s.indexOf("Name") -1).trim(), "Questions Bronze 0 Questions Silver 0 Questions Gold 0 Answers Bronze 0 Answers Silver 1 Answers Gold 0");
						else if	(level == 3)   
							return new Tuple2<String, String>(s.substring(s.indexOf("UserId"), s.indexOf("Name") -1).trim(), "Questions Bronze 0 Questions Silver 0 Questions Gold 0 Answers Bronze 0 Answers Silver 0 Answers Gold 1");	
						return new Tuple2<String, String>("NoQA", "Questions Bronze 0 Questions Silver 0 Questions Gold 0 Answers Bronze 0 Answers Silver 0 Answers Gold 0");	
					}

					else {
						if (level == 1)  
							return new Tuple2<String, String>(s.substring(s.indexOf("UserId"), s.indexOf("Name") -1).trim(), "Questions Bronze 1 Questions Silver 0 Questions Gold 0 Answers Bronze 0 Answers Silver 0 Answers Gold 0");
						else if (level == 2)  
							return new Tuple2<String, String>(s.substring(s.indexOf("UserId"), s.indexOf("Name") -1).trim(), "Questions Bronze 0 Questions Silver 1 Questions Gold 0 Answers Bronze 0 Answers Silver 0 Answers Gold 0");
						else if	(level ==3)  
							return new Tuple2<String, String>(s.substring(s.indexOf("UserId"), s.indexOf("Name") -1).trim(), "Questions Bronze 0 Questions Silver 0 Questions Gold 1 Answers Bronze 0 Answers Silver 0 Answers Gold 0");			
						return new Tuple2<String, String>("NoQA", "Questions Bronze 0 Questions Silver 0 Questions Gold 0 Answers Bronze 0 Answers Silver 0 Answers Gold 0");	
					}
				}
				return new Tuple2<String, String>("0nope", "0,0,0,0,0,0"); // fail-safe dummy statement
			}
		});

		JavaPairRDD<String, String> counts = values.reduceByKey(new Function2<String, String, String>() {
			@Override
			public String call(String a, String b) {
				int[] aInts = new int[6];
				int[] bInts = new int[6];
				
				if(a.charAt(0) == 'Q'){
					String[] aSplit = a.split(" ");
					for(int i = 0; i < 6; i++) {
						aInts[i] = Integer.parseInt(aSplit[(i*3) + 2]);
					}
				}
				else{
					String[] aSplit = a.split(",");
					for(int i = 0; i < 6; i++){
						aInts[i] = Integer.parseInt(aSplit[i]);
					}
				}
				
				if(b.charAt(0) == 'Q'){	
					String[] bSplit = b.split(" ");
					for(int i = 0; i < 6; i++) {
						bInts[i] = Integer.parseInt(bSplit[(i*3) + 2]);
					}
				}
				else{
					String[] bSplit = b.split(",");
					for(int i = 0; i < 6; i++){
						bInts[i] = Integer.parseInt(bSplit[i]);
					}
				}

				int[] c = new int[6];
				for (int i = 0; i < c.length; ++i) {
					c[i] = aInts[i] + bInts[i];
				}
				return "Questions Bronze " + c[0] + " Questions Silver " + c[1] + " Questions Gold " + c[2] + " Answers Bronze " + c[3] + " Answers Silver " + c[4] + " Answers Gold " + c[5];
			}
		});
		
		
		counts.saveAsTextFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/user/aa327/sparkcount"); //save on hdfs to folder named sparkcount in user dir "ec09075" <-- MUST BE CHANGED
		ctx.stop();
	}
}
