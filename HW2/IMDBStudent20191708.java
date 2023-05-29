import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

class Info {
	public String movie_title;
	public double average;
	
	public Info(String movie_title, double average) {
		this.movie_title = movie_title;
		this.average = average;
	}

	public String getTitle() {
		return this.movie_title;
	}
	
	public double getAverage() {
		return this.average;
	}

	public String getString() {
		return movie_title + " " + average;
	}
}

class DoubleString implements WritableComparable {
	String joinKey = new String();
	String tableName = new String();
	public DoubleString() {}
	public DoubleString(String _joinKey, String _tableName) {
		joinKey = _joinKey;
		tableName = _tableName;
	}
	public void readFields(DataInput in) throws IOException {
		joinKey = in.readUTF();
		tableName = in.readUTF();
	}
	public void write(DataOutput out) throws IOException {
		out.writeUTF(joinKey);
		out.writeUTF(tableName);
	}
	
	public int compareTo(Object o1) {
		DoubleString o = (DoubleString) o1;
		int ret = joinKey.compareTo(o.joinKey);
		if (ret != 0) return ret;
		return tableName.compareTo(o.tableName);
	}
	public String toString() { 
		return joinKey + " " + tableName; 
	}
}

public class IMDBStudent20191708 {
	public static class AverageComparator implements Comparator<Info> {
		public int compare(Info x, Info y) {
			if ( x.average > y.average ) return 1;
			if ( x.average < y.average ) return -1;
			return 0;
		}
	}
	
	public static void insertInfo(PriorityQueue q, String movie_title, double average, int topK) {
		Info info_head = (Info) q.peek();
		if (q.size() < topK || info_head.average < average) {
			Info info = new Info(movie_title, average);
			q.add(info);
			
			if(q.size() > topK) q.remove();
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
			if(0 == result) {
				result = k1.tableName.compareTo(k2.tableName);
			}
			return result;
			}
	}
	
	public static class FirstPartitioner extends Partitioner<DoubleString, Text> {
		public int getPartition(DoubleString key, Text value, int numPartition) {
			return key.joinKey.hashCode()%numPartition;
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
			String [] valSplited = value.toString().split("::");
			DoubleString outputKey = null;
			Text outputValue = new Text();
			if (movieFile) {
				String movie_id = valSplited[0];
				String movie_title = valSplited[1];
				String movie_genre = valSplited[2];
				
				StringTokenizer itr = new StringTokenizer(movie_genre, "|");
				boolean isFantasy = false;
				while (itr.hasMoreElements()) {
					if(itr.nextToken().equals("Fantasy")) {
						isFantasy = true;
						break;
					}
				}
				
				if (isFantasy) {
					outputKey = new DoubleString(movie_id, "M");
					outputValue.set("M," + movie_title);
					context.write(outputKey, outputValue);
				}
			} else {
				String movie_id = valSplited[1];
				String movie_rate = valSplited[2];
				
				outputKey = new DoubleString(movie_id, "R");
				outputValue.set("R," + movie_rate);
				context.write(outputKey, outputValue);
			}
		}

		protected void setup(Context context) throws IOException, InterruptedException {
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if ( filename.indexOf( "movies.dat" ) != -1 ) movieFile = true;
			else movieFile = false;
		}
	}
	
	public static class IMDBReducer extends Reducer<DoubleString, Text, Text, DoubleWritable> {
		private PriorityQueue<Info> queue;
		private Comparator<Info> comp = new AverageComparator();
		private int topK;
		public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String movie_title = "";
			int total_rate = 0;
			int i = 0;
			for(Text val : values) {
				String data = val.toString();
				String [] splited = data.split(",");
				
				if(i == 0) {
					if(!splited[0].equals("M")) break;
					movie_title = splited[1];
				} else {
					total_rate += Integer.valueOf(splited[1]);
				}
				i++;
			}
			
			if (total_rate != 0) {
				double average = ((double)total_rate) / (i - 1);
				insertInfo(queue, movie_title, average, topK);
			}
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Info>(topK , comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			while(queue.size() != 0) {
				Info info = (Info) queue.remove();
				context.write(new Text(info.getTitle()), new DoubleWritable(info.getAverage()));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: <in> <out> <topK>");
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

