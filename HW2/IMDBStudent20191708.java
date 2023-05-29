import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

class MovieInfo {
	public String title;
	public double average;

	public MovieInfo(String title, double average) {
		this.title = title;
		this.average = average;
	}

	public String getTitle() {
		return this.title;
	}

	public double getAverage() {
		return this.average;
	}

	public String getString() {
		return title + " " + average;
	}
}


class DoubleString implements WritableComparable {
	String joinKey = new String();
	String tableName = new String();

	public DoubleString() {}
	public DoubleString(String jKey, String tName) {
		joinKey = jKey;
		tableName = tName;
	}

	public void readFields(DataInput in) throws IOException {
		joinKey = in.readUTF();
		tableName = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(joinKey);
		out.writeUTF(tableName);
	}

	public int compareTo(Object obj) {
		DoubleString ob = (DoubleString)obj;
		int ret = joinKey.compareTo(ob.joinKey);

		if (ret != 0)
			return ret;
		return tableName.compareTo(ob.tableName);
	}

	public String toString() {
		return joinKey + " " + tableName;
	}
}

public class IMDBStudent20191708 {
	public static class AverageComparator implements Comparator<MovieInfo> {
		public int compare(MovieInfo m1, MovieInfo m2) {
			if (m1.average > m2.average)
				return 1;
			if (m1.average < m2.average)
				return -1;
			return 0;
		}
	}

	public static void insertMovieInfo(PriorityQueue q, String title, double average, int topK) {
		MovieInfo infoHead = (MovieInfo)q.peek();
		if (q.size() < topK || infoHead.average < average) {
			MovieInfo mInfo = new MovieInfo(title, average);
			q.add(minfo);

			if (q.size() > topK)
				q.remove();
		}
	}

	public static class CompositeKeyComparator extends WritableComparator {
		protected CompositeKeyComparator() {
			super(DoubleString.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;
			int result = k1.joinKey.compareTo(k2.joinKey);

			if(result == 0)
				result = k1.tableName.compareTo(k2.tableName);
			return result;
		}
	}

	public static class FirstPartitioner extends Partitioner<DoubleString, Text> {
		public int getPartition(DoubleString key, Text value, int numPartition) {
			return key.joinKey.hashCode() % numPartition;
		}
	}

	public static class FirstGroupingComparator extends WritableComparator {
		protected FirstGroupingComparator() {
			super(DoubleString.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString)w1;
			DoubleString k2 = (DoubleString)w2;
			return k1.joinKey.compareTo(k2.joinKey);
		}
	}

	public static class IMDBMapper extends Mapper<Object, Text, DoubleString, Text> {
		boolean movieFile = true;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String [] token = value.toString().split("::");
			DoubleString outputKey = null;
			Text outputValue = new Text();

			if(movieFile) {
				String id = token[0];
				String title = token[1];
				String genre = token[2];
				StringTokenizer itr = new StringTokenizer(genre, "|");
				boolean isFantasy = false;

				while (itr.hasMoreElements()) {
					if(itr.nextToken().equals("Fantasy")) {
						isFantasy = true;
						break;
					}
				}

				if (isFantasy) {
					outputKey = new DoubleString(id, "M");
					outputValue.set("M," + title);
					context.write( outputKey, outputValue );
				}
			} else {
				Stirng id = token[1];
				String rate = token[2];
				outputKey = new DoubleString(id, "R");
				outputValue.set("R," + rate);
				context.write(outputKey, outputValue);
			}
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			String fileName = ((FileSplit)context.getInputSplit()).getPath().getName();
			if (fileName.indexOf("movies.dat") != -1)
				movieFile = true;
			else
				movieFile = false;
		}
	}

	public static class IMDBReducer extends Reducer<DoubleString,Text,Text,DoubleWritable> {
		private PriorityQueue<MovieInfo> queue;
		private Comparator<MovieInfo> comp = new AverageComparator();
		private int topK;

		public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String title = "";
			int rate = 0;
			int i = 0;
			for (Text val : values) {
				String data = val.toString();
				String [] s = data.split(",");

				if (i == 0) {
					if(!s[0].equals("M"))
						break;
					title = s[1];
				} else {
					rate += Integer.valueOf(s[1]);
				}
				i++;
			}

			if (rate != 0) {
				double average = ((double)rate) / (i - 1);
				insertMovieInfo(queue, title, average, topK);
			}
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<MovieInfo>(topK, comp);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				MovieInfo mInfo = (MovieInfo)queue.remove();
				context.write(new Text(mInfo.getTitle()), new DoubleWritable(mInfo.getAverage()));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: IMDB <in> <out> <topK>");
			System.exit(2);
		}
		conf.setInt("topK", Integer.valueOf(otherArgs[2]));
		Job job = new Job(conf, "IMDB");
		job.setJarByClass(IMDBStudent20191708.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(DoubleString.class);
		job.setMapOutputValueClass(Text.class);
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
