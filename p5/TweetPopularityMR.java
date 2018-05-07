package cmsc433.p5;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Map reduce which takes in a CSV file with tweets as input and output
 * key/value pairs.</br>
 * </br>
 * The key for the map reduce depends on the specified {@link TrendingParameter}
 * , <code>trendingOn</code> passed to
 * {@link #score(Job, String, String, TrendingParameter)}).
 */
public class TweetPopularityMR {

	// For your convenience...
	public static final int          TWEET_SCORE   = 1;
	public static final int          RETWEET_SCORE = 2;
	public static final int          MENTION_SCORE = 1;
	public static final int			 PAIR_SCORE = 1;

	// Is either USER, TWEET, HASHTAG, or HASHTAG_PAIR. Set for you before call to map()
	private static TrendingParameter trendingOn;

	public static class TweetMapper
	extends Mapper<LongWritable,Text,Text,LongWritable/* TODO: fill in the generic type arguments */> {
		
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// Converts the CSV line into a tweet object
			Tweet tweet = Tweet.createTweet(value.toString());
			switch(trendingOn) {
			case USER:
				context.write((new Text(tweet.getUserScreenName())), new LongWritable(TWEET_SCORE));

				if(tweet.wasRetweetOfUser()) {
					context.write(new Text(tweet.getRetweetedUser()), new LongWritable(RETWEET_SCORE));
				}
				if(tweet.getMentionedUsers() != null) {
					for (String mentionedUser : tweet.getMentionedUsers()) {
						context.write(new Text(mentionedUser), new LongWritable(MENTION_SCORE));
					}
				}
				break;
			case TWEET:
				context.write(new Text(tweet.getId().toString()), new LongWritable(1));
				if(tweet.getRetweetedTweet()!=null) {
					context.write(new Text(tweet.getRetweetedTweet().toString()), new LongWritable(RETWEET_SCORE));
				}
			
				break;
			case HASHTAG:
				for (String hashtag : tweet.getHashtags()) {
					context.write(new Text(hashtag), new LongWritable(1));
				}
				
				break;
			case HASHTAG_PAIR:
				
				
				break;
			default:
					break;
			}
			
			




		}
	}

	public static class PopularityReducer
	extends Reducer<Text,IntWritable,Text,IntWritable/* TODO: fill in the generic type arguments */> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// TODO: Your code goes here




		}
	}

	/**
	 * Method which performs a map reduce on a specified input CSV file and
	 * outputs the scored tweets, users, or hashtags.</br>
	 * </br>
	 * 
	 * @param job
	 * @param input
	 *          The CSV file containing tweets
	 * @param output
	 *          The output file with the scores
	 * @param trendingOn
	 *          The parameter on which to score
	 * @return true if the map reduce was successful, false otherwise.
	 * @throws Exception
	 */
	public static boolean score(Job job, String input, String output,
			TrendingParameter trendingOn) throws Exception {

		TweetPopularityMR.trendingOn = trendingOn;

		job.setJarByClass(TweetPopularityMR.class);

		// TODO: Set up map-reduce...
		
		
		// Configure key output classes for the job classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// Configure the Mapper and Reducer classes
		job.setMapperClass(TweetPopularityMR.TweetMapper.class);
		job.setReducerClass(TweetPopularityMR.PopularityReducer.class);
		
		// Configure Input format class
		job.setInputFormatClass(FileInputFormat.class);
		job.setOutputFormatClass(FileOutputFormat.class);




		// End

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job.waitForCompletion(true);
	}
}
