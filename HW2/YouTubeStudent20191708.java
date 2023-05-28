import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

class Video {
	public String genre;
	public double average;
	
	public Video(String genre, double average) {
		this.genre = genre;
		this.average = average;
	}
	
	public String getGenre() {
		return this.genre;
	}
	
	public double getAverage() {
		return this.average;
	}
}

public class YouTubeStudent20191708 {
	public static class AverageComparator implements Comparator<Video> {
		public int compare(Video v1, Video v2) {
			if (v1.average > v2.average)
				return 1;
			if (v1.average < v2.average)
				return -1;
			return 0;
		}
	}
	
	public static void insertVideo(PriorityQueue q, String genre, double average, int topK) {
		Video videoHead = (Video)q.peek();
		if (q.size() < topK || videoHead.average < average ) {
			Video video = new Video(genre, average);
			q.add(video);
			if(q.size() > topK) q.remove();
		}
	}
	
	public static class YoutubeMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), "|");
			String genre = "";
			String rate = "";
			int i = 0;

			while(itr.hasMoreTokens()) {
				rate = itr.nextToken();
				if(i == 3)
					genre = rate;
				i++;
			}
			context.write(new Text(genre), new DoubleWritable(Double.valueOf(rate)));
		}
	}
	
	public static class YoutubeReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private PriorityQueue<Video> queue;
		private Comparator<Video> comp = new AverageComparator();
		private int topK;

		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			double sum = 0;
			int size = 0;
			
			for(DoubleWritable val : values) {
				sum += val.get();
				size++;
			}
			double average = sum / size;

			insertVideo(queue, key.toString(), average, topK);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Video>(topK , comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				Video video = (Video)queue.remove();
				context.write(new Text(video.getGenre()), new DoubleWritable(video.getAverage()));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: YouTube <in> <out>");
			System.exit(2);
		}
		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
		Job job = new Job(conf, "YouTube");
		job.setJarByClass(YouTubeStudent20191708.class);
		job.setMapperClass(YoutubeMapper.class);
		job.setReducerClass(YoutubeReducer.class);
		job.setNumReduceTasks(1);	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
