import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1
{
	public static class MapperClass
		extends Mapper<Object, Text, Text, ArrayPrimitiveWritable>
	{
		private Text resultKey = new Text();
		private ArrayPrimitiveWritable resultValue = new ArrayPrimitiveWritable();

		public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
			{
				long[] resultArray = new long[2];
				String[] parts;
				String tweet;
				long likes;
				long reTweets;

				try
				{
					parts = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
					tweet = parts[2];
					likes = (long) Double.parseDouble(parts[3]);
					reTweets = (long) Double.parseDouble(parts[4]);
					resultArray[0] = likes;
					resultArray[1] = reTweets;
					if (tweet.contains("#JoeBiden") || tweet.contains("#Biden"))
					{
						resultKey.set("Biden");
						resultValue.set(resultArray);
						context.write(resultKey, resultValue);
					}
					if (tweet.contains("#DonaldTrump") || tweet.contains("#Trump"))
					{
						resultKey.set("Trump");
						resultValue.set(resultArray);
						context.write(resultKey, resultValue);

					}
				}
				catch(Exception e)
				{
					resultKey.set("error");
					resultArray[0] = 1;
					resultArray[1] = 0;
					resultValue.set(resultArray);
					context.write(resultKey, resultValue);
				}
			}	
	}
	
	public static class CombinerClass
		extends Reducer<Text, ArrayPrimitiveWritable, Text, ArrayPrimitiveWritable>
	{
		private ArrayPrimitiveWritable resultValue = new ArrayPrimitiveWritable();

		public void reduce(Text key, Iterable<ArrayPrimitiveWritable> values, Context context
							) throws IOException, InterruptedException 
		{
			long[] resultArray = new long[2];
			long likes = 0;
			long reTweets = 0;
			for (ArrayPrimitiveWritable val : values) 
			{
				long[] valueArray = (long[]) val.get();
				likes += valueArray[0];
				reTweets += valueArray[1];
			}
			resultArray[0] = likes;
			resultArray[1] = reTweets;
			resultValue.set(resultArray);
			
			context.write(key, resultValue);
		}
	}

	public static class ReducerClass
		extends Reducer<Text, ArrayPrimitiveWritable, Text, Text>
	{
		private Text resultValue = new Text();

		public void reduce(Text key, Iterable<ArrayPrimitiveWritable> values, Context context
							) throws IOException, InterruptedException 
		{
			StringBuilder resultString = new StringBuilder();
			long likes = 0;
			long reTweets = 0;
			for (ArrayPrimitiveWritable val : values) 
			{
				long[] valueArray = (long[]) val.get();
				likes += valueArray[0];
				reTweets += valueArray[1];
			}
			resultString.append(String.valueOf(likes) + " " + String.valueOf(reTweets));
			resultValue.set(resultString.toString());
			
			context.write(key, resultValue);
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Question 1");
		job.setJarByClass(Q1.class);
		job.setMapperClass(MapperClass.class);
		job.setCombinerClass(CombinerClass.class);
		job.setReducerClass(ReducerClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(ArrayPrimitiveWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}