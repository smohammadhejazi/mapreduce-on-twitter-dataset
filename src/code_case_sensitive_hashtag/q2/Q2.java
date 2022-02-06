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

public class Q2
{
	final static String[] countryArray = {"AMERICA", "IRAN", "NETHERLANDS", "AUSTRIA",
	"MEXICO", "EMIRATES", "FRANCE", "GERMANY", "ENGLAND", "CANADA", "SPAIN", "ITALY"};
	
	public static class MapperClass
		extends Mapper<Object, Text, Text, ArrayPrimitiveWritable>
	{
		private Text resultKey = new Text();
		private ArrayPrimitiveWritable resultValue = new ArrayPrimitiveWritable();

		public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException 
			{
				long[] resultArray = new long[4];
				String[] parts;
				String tweet;
				String country;

				try
				{
					parts = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
					tweet = parts[2];
					country = parts[16];

					if (tweet.contains("#JoeBiden") || tweet.contains("#Biden"))
					{
						resultArray[0] = 1;
					}
					if (tweet.contains("#DonaldTrump") || tweet.contains("#Trump"))
					{
						resultArray[1] = 1;
					}
					if (resultArray[0] == 1 && resultArray[1] == 1)
					{
						resultArray[2] = 1;
					}
					resultArray[3] = 1;
					
					for (String s : countryArray)
					{
						if (country.toUpperCase().contains(s))
						{
							resultKey.set(s + "\t\t");
							break;
						}
					}
					if (!resultKey.toString().equals(""))
					{
						resultValue.set(resultArray);
						context.write(resultKey, resultValue);
					}
				}
				catch(Exception e)
				{
					resultKey.set("ERROR");
					resultArray[0] = 0;
					resultArray[1] = 0;
					resultArray[2] = 0;
					resultArray[3] = 1;
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
			long[] resultArray = new long[4];
			
			for (ArrayPrimitiveWritable val : values) 
			{
				long[] valueArray = (long[]) val.get();
				resultArray[0] += valueArray[0];
				resultArray[1] += valueArray[1];
				resultArray[2] += valueArray[2];
				resultArray[3] += valueArray[3];
			}
			
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
			long tweet = 0;
			long both = 0;
			long biden = 0;
			long trump = 0;
			double bothPercentage = 0;
			double bidenPercentage = 0;
			double trumpPercentage = 0;

			
			for (ArrayPrimitiveWritable val : values) 
			{
				long[] valueArray = (long[]) val.get();
				biden += valueArray[0];
				trump += valueArray[1];
				both += valueArray[2];
				tweet += valueArray[3];

			}
			bothPercentage = ((double) both / tweet);
			bidenPercentage = ((double) biden / tweet);
			trumpPercentage = ((double) trump / tweet);
			
			resultValue.set(String.format("%-20s %-20s %-20s %-20s",
			String.valueOf(bothPercentage),
			String.valueOf(bidenPercentage),
			String.valueOf(trumpPercentage),
			String.valueOf(tweet)));
			
			context.write(key, resultValue);
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Question 2");
		job.setJarByClass(Q2.class);
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