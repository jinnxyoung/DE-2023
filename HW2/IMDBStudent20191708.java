import java.io.*;
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

public class IMDBStudent20191708 {
	public static class MovieInfo {
		public String title;
		public double average;
		
		public MovieInfo (String title, double average) {
			this.title = title;
			this.average = average;
		}
		
		public String getTitle() {
			return this.title;
		}

		public double getAvgRating() {
			return this.average;
		}
		
	}

	public static class DoubleString implements WritableComparable {
		String movieId = new String();
		String tableName = new String();

		public DoubleString() {}

		public DoubleString(String _movieId, String _tableName) {
			super();
			this.movieId = _movieId;
			this.tableName = _tableName;
		}
		
		public void readFields(DataInput in) throws IOException{
			movieId = in.readUTF();
			tableName = in.readUTF();
		}

		public void write(DataOutput out) throws IOException{
			out.writeUTF(movieId);
			out.writeUTF(tableName);
		}
		
		public int compareTo(Object obj) {
			DoubleString o = (DoubleString) obj;
			int ret = movieId.compareTo(o.movieId);
			if (ret != 0) return ret;
			return tableName.compareTo(o.tableName);
		}

		public String toString(){
			return movieId + " " + tableName;
		}
	}

	public static class FirstPartitioner extends Partitioner<DoubleString, Text>{
		public int getPartition(DoubleString key, Text value, int numPartition) {
			return key.movieId.hashCode()%numPartition;
		}
	}

	public static class FirstGroupingComparator extends WritableComparator{
		protected FirstGroupingComparator() {
			super(DoubleString.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString) w1;
			DoubleString k2 = (DoubleString) w2;
			return k1.movieId.compareTo(k2.movieId);
		}
	}
	public static class CompositeKeyComparator extends WritableComparator{
		protected CompositeKeyComparator() {
			super(DoubleString.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			DoubleString k1 = (DoubleString) w1;
			DoubleString k2 = (DoubleString) w2;
			
			int result = k1.movieId.compareTo(k2.movieId);
			if (0 == result) {
				result = k1.tableName.compareTo(k2.tableName);
			}
			return result;
		}
	}

	public static class IMDBMapper extends Mapper<Object, Text, DoubleString, Text>{
		boolean isMovie = true;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String v = value.toString();
			String[] val = v.split("::"); 
			if (val.length > 1){
				DoubleString outputK = new DoubleString();
				Text outputV = new Text();
	 			if (isMovie) {
					String movieId = val[0].trim();
					String title = val[1];
					String genres = val[2].trim();
					boolean isFantasy = false;
					StringTokenizer itr = new StringTokenizer(genres, "|");
					
					while(itr.hasMoreTokens()){
						if((itr.nextToken()).equals("Fantasy")){
							isFantasy = true;
							break;
						}
					}
					if (isFantasy) {
						outputK = new DoubleString(movieId, "Movies");
						outputV.set("Movies::" + title);
						context.write(outputK, outputV);
					}
				} else {
					String movieId = val[1];
					String avgRating = val[2];
					outputK = new DoubleString(movieId, "Ratings");
					outputV.set("Ratings::" + avgRating);
					context.write(outputK, outputV);
				}
			}
		}
				
		protected void setup(Context context) throws IOException, InterruptedException{
			String filename = ((FileSplit)context.getInputSplit()).getPath().getName();
			if (filename.indexOf("movies.dat") != -1) isMovie = true;
   			else isMovie = false;
		}
	}

	public static class IMDBReducer extends Reducer<DoubleString, Text, Text, DoubleWritable> {
		private PriorityQueue<MovieInfo> queue;
		private Comparator<MovieInfo> comp = new MovieComparator();
		private int topK;
		
		public void reduce(DoubleString key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int count = 0;
			double sum = 0;
			String movieTitle = "";
			
			for (Text val : values) {
				String[] v = val.toString().split("::");
				String fileName = v[0];
				if (count == 0) {
					if(fileName.equals("Ratings")) {
						break;
					}
					movieTitle = v[1];
				} else {
					sum += Double.parseDouble(v[1]);
				}
				count++;
			}
			if (sum != 0){
				double avg = sum / (count - 1);
				insertQueue(queue, movieTitle, avg, topK);
			}
		}
		
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK" , -1);
			queue = new PriorityQueue<MovieInfo> (topK, comp);
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			while (queue.size() != 0) {
				MovieInfo movie = (MovieInfo)queue.remove();
				context.write(new Text(movie.getTitle()), new DoubleWritable(movie.getAvgRating())); 
			}
		}
	}
	
	public static class MovieComparator implements Comparator<MovieInfo> {
		@Override
		public int compare(MovieInfo o1, MovieInfo o2) {
			if(o1.average > o2.average) return 1;
			if(o1.average < o2.average) return -1; 	
			else return 0;
		}
	}
	
	public static void insertQueue(PriorityQueue q, String title, double avgRating, int topK) {
		MovieInfo head = (MovieInfo)q.peek();
		if(q.size() < topK || head.average < avgRating) {
			MovieInfo movie = new MovieInfo(title, avgRating);
			q.add(movie);
			if(q.size() > topK) q.remove();
		}
	}
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if(otherArgs.length != 3){
			System.err.println("Usage: <in> <out> <topK>"); 
   			System.exit(2);
		}
		conf.setInt("topK", Integer.parseInt(otherArgs[2]));
		
		Job job = new Job(conf, "IMDBStudent20191708");
		job.setJarByClass(IMDBStudent20191708.class);
		job.setMapperClass(IMDBMapper.class);
		job.setReducerClass(IMDBReducer.class);
		
		job.setPartitionerClass(FirstPartitioner.class);
		job.setGroupingComparatorClass(FirstGroupingComparator.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		
		job.setMapOutputKeyClass(DoubleString.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete(new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	
	}
}
