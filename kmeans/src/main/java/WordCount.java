import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class WordCount {
	
	private static final Pattern SPACE = Pattern.compile("/>");

	public static void main(String[] args) throws Exception {
		JavaSparkContext ctx = new JavaSparkContext("yarn-client", "WordCount", "/usr/lib/spark/", new String[]{"target/WordCount-1.0.jar"});
		String url = "hdfs://hadoop.eecs.qmul.ac.uk:54310/user/aa327/finaljoindata";
		JavaRDD<String> file = ctx.textFile(url, 1).flatMap(new FlatMapFunction<String, String>() {
			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});

		JavaRDD<User> users = file.map(new Function<String, User>() {
			@Override
                	public User call(String line) {
				String[] split = line.split(",");
				String id = split[0].replaceAll("[^\\d]", "");
                    		String[] values = split[1].replaceAll("[a-zA-Z]*[^\\s\\d]", "").split(" ");

                    		return new User(id, values);
                	}
            	});

		Set<User> group1 = new HashSet<User>();
		Set<User> group2 = new HashSet<User>();
		
		User user1 = users.takeSample(false, 1, 50).get(0);
		User user2 = users.takeSample(false, 1, 100).get(0);

//		System.out.println(user1.toString());
//		System.out.println(user2.toString());
//		System.out.println(user1.compareTo(user2));

		List<User> allUsers = users.collect();
		for(User user : allUsers) {
			if(user.compareTo(user1) < user.compareTo(user2)) {
				group1.add(user);
			}
			else {
				group2.add(user);		
			}
		}

		User group1Mean = null, group2Mean = null;
		
		// 1000 iteration fixen KMeans.
		// Can be asily implemented to compare
		// previous values of the 2 means, converge
		// after certain threshold.
		int iter = 1000;
		while(iter != 0) {
			iter--;

			int meanVR1 = 0, meanPR1 = 0, meanQR1 = 0, meanAR1 = 0;
			for(User user : group1) {
				meanVR1 += user.getVoteRatio();
				meanPR1 += user.getPostRatio();
				meanQR1 += user.getQuestionRatio();
				meanAR1 += user.getAnswerRatio();
			}
			meanVR1 = meanVR1 / group1.size();
			meanPR1 = meanPR1 / group1.size();
			meanQR1 = meanQR1 / group1.size();
			meanAR1 = meanAR1 / group1.size();

			int meanVR2 = 0, meanPR2 = 0, meanQR2 = 0, meanAR2 = 0;
			for(User user : group2) {
				meanVR2 += user.getVoteRatio();
				meanPR2 += user.getPostRatio();
				meanQR2 += user.getQuestionRatio();
				meanAR2 += user.getAnswerRatio();
			}
			meanVR2 = meanVR2 / group2.size();
			meanPR2 = meanPR2 / group2.size();
			meanQR2 = meanQR2 / group2.size();
			meanAR2 = meanAR2 / group2.size();

			group1Mean = new User(meanVR1, meanPR1, meanQR1, meanAR1);
			group2Mean = new User(meanVR2, meanPR2, meanQR2, meanAR2);
			
			Set<User> removeCandidates1 = new HashSet<User>();
			for(User user : group1) {
				if(user.compareTo(group1Mean) > user.compareTo(group2Mean)) {
					removeCandidates1.add(user);
					group2.add(user);
				}
			}
			group1.removeAll(removeCandidates1);
		
			Set<User> removeCandidates2 = new HashSet<User>();
			for(User user : group2) {
				if(user.compareTo(group2Mean) > user.compareTo(group1Mean)) {
					removeCandidates2.add(user);
					group1.add(user);
				}
			}
			group2.removeAll(removeCandidates2);
		}
	
		for(User user : group1) {
			System.out.println(user.toString());
			break;
		}
		for(User user : group2) {
			System.out.println(user.toString());
			break;
		}

		System.out.println(group1Mean.getVoteRatio() + " " + group1Mean.getPostRatio() + " " + group1Mean.getQuestionRatio() + " " + group1Mean.getAnswerRatio());
		System.out.println("There are " + group1.size() + " users in group 1");
		System.out.println(group2Mean.getVoteRatio() + " " + group2Mean.getPostRatio() + " " + group2Mean.getQuestionRatio() + " " + group2Mean.getAnswerRatio());
		System.out.println("There are " + group2.size() + " users in group 2");
	
		//users.saveAsTextFile("hdfs://hadoop.eecs.qmul.ac.uk:54310/user/aa327/finaljoindata");
		ctx.stop();
	}
}


class User implements Comparable<User> {
	int id, rep, postC, upV, downV;
	int qB, qS, qG, aB, aS, aG;
	int questionRatio;
	int answerRatio;
	int voteRatio;
	int postRatio;
		
	public User(String id, String[] values) {
		this.id = Integer.parseInt(id);
		rep = Integer.parseInt(values[0]);
		upV = Integer.parseInt(values[1]);
		downV = Integer.parseInt(values[2]);
		qB = Integer.parseInt(values[3]);
		qS = Integer.parseInt(values[4]);
		qG = Integer.parseInt(values[5]);
		aB = Integer.parseInt(values[6]);
		aS = Integer.parseInt(values[7]);
		aG = Integer.parseInt(values[8]);
		postC = Integer.parseInt(values[9]);

		postRatio = Integer.parseInt(values[10]);
		answerRatio = (aB + (aS*2) + (aG*3))/6;
		questionRatio = (qB + (qS*2) + (qG*3))/6;
		voteRatio = upV - downV;
	}

	public User(int voteRatio, int postRatio, int questionRatio, int answerRatio) {
		this.voteRatio = voteRatio;
		this.postRatio = postRatio;
		this.questionRatio = questionRatio;
		this.answerRatio = answerRatio;
	}

	public int getVoteRatio() {
		return voteRatio;	
	}

	public int getPostRatio() {
		return postRatio;
	}

	public int getAnswerRatio() {
		return answerRatio;
	}	

	public int getQuestionRatio() {
		return questionRatio;
	}

	@Override
	public String toString() {
		String s = "UserId=" + id + ", Reputation=" + rep + " PostCount=" + postC + " PostRatio=" + postRatio
			   + " UpVotes=" + upV + " DownVotes=" + downV + " Questions={Bronze=" + qB + " Silver=" + qS
			   + " Gold=" + qG + "} Answers={Bronze=" + aB + " Silver=" + aS + " Gold=" + aG + "}";	
		return(s);
	}

	@Override
	public int compareTo(User otherUser) {
		int voteDiff = this.getVoteRatio() - otherUser.getVoteRatio();
		int postDiff = this.getPostRatio() - otherUser.getPostRatio();
		int answerDiff = this.getAnswerRatio() - otherUser.getAnswerRatio();
		int questionDiff = this.getQuestionRatio() - otherUser.getQuestionRatio();		
			
		return Math.abs(voteDiff + postDiff + answerDiff + questionDiff);
	}
}

