package classification;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class MapReduce {

	public static class FirstMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] arr = line.split(",");
			String r = arr[arr.length - 1];
			IntWritable RSSI = new IntWritable(Integer.parseInt(r));

			// get RSSI
			context.write(RSSI, value);
		}
	}

	public static class FirstReducer extends
			Reducer<IntWritable, Text, IntWritable, Text> {

		Context cont;
		TaskAttemptID redId;
		BufferedWriter bw;

		@Override
		public void setup(Context context) throws IOException {
			cont = context;
			redId = context.getTaskAttemptID();

			// create new file of RSSI
			Path pt = new Path("hdfs://" + Wchecker.host + ":9000" + Wchecker.RSSI
					+ redId + ".txt");
			FileSystem fs = FileSystem.get(new Configuration());
			bw = new BufferedWriter(new OutputStreamWriter(fs.create(pt)));
		}

		@Override
		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int count = 0;

			// count number of the RSSI
			for (Text val : values) {
				count++;
			}

			// write RSSI,number of RSSI
			String out = String.valueOf(key) + "," + String.valueOf(count)
					+ "\n";
			bw.write(out);
		}

		@Override
		public void cleanup(Context context) throws IOException {
			bw.close();
		}

	}

	public static class SecondMapper extends
			Mapper<LongWritable, Text, IntWritable, Text> {
		ArrayList<String> grade = new ArrayList<String>();
		
		@Override
		public void setup (Context context) throws IOException{
			Path p = new Path("hdfs://" + Wchecker.host + ":9000" + 
		Wchecker.RSSI + "final.txt");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(
					fs.open(p)));
			
			String line = br.readLine();
			String[] g = line.split("/");
			
			for (int i=0; i<g.length; i++){
				grade.add(g[i]);
			}
			
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Boolean check = true;
			String line = value.toString();
			String[] arr = line.split(",");
			String r = arr[arr.length - 1];
			int RSSI = Integer.parseInt(r);
			int final_grade = 0;
			
			for (int i=0; i<Wchecker.GRADENUM - 1; i++){
				String bound = grade.get(i);
				int start = Integer.parseInt(bound.split(",")[0]);
				int end = Integer.parseInt(bound.split(",")[1]);
				
				if (end <= RSSI && RSSI <= start){
					final_grade = i+1;
					check = false;
					break;
				}// end of if
			}// end of for
			
			if ( check )
				final_grade = Wchecker.GRADENUM;
			
			IntWritable result = new IntWritable(final_grade);
			context.write(result, value);	
		}
	}
}

