package classification;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import classification.MapReduce.FirstMapper;
import classification.MapReduce.FirstReducer;
import classification.MapReduce.SecondMapper;

public class Wchecker {
	public static int iteration;
	public static final String host = "avocado";
	public static final String RSSI = "/kwangmin/output/rssi/";
	public static final String OUT = "/kwangmin/output/result.txt";
	public static int max;
	public static int min;
	public static int count;
	public static int total;
	public static final int GRADENUM = 5;
	public static final int REDUCE_NUM = 3;
	
	// main
	public static void main(String[] args) throws Exception {
		ArrayList<String> rssi_list = new ArrayList<String>();
		iteration = 0;

		// first : job for getting RSSIs and number of each RSSI
		if (iteration == 0) {
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Wchecker");
			job.setJarByClass(Wchecker.class);
			job.setMapperClass(FirstMapper.class);
			job.setReducerClass(FirstReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.setNumReduceTasks(REDUCE_NUM);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]
					+ "/classification_first"));
			
			job.waitForCompletion(true);

			iteration++;
		}

		// second : estimate grades
		// read files in RSSI directory
		FileSystem f = FileSystem.get(new Configuration());
		FileStatus[] list = f.listStatus(new Path("hdfs://" + host + ":9000" + RSSI));
		max = -200;
		min = 0;
		count = 0;
		total = 0;

		// merge all RSSI's information 
		for (int i = 0; i < list.length; i++) {
			BufferedReader br = new BufferedReader(new InputStreamReader(
					f.open(list[i].getPath())));

			// store RSSI in ArrayList : rssi_list
			String line = br.readLine();
			while (line != null) {
				rssi_list.add(line);
				int val = Integer.parseInt(line.split(",")[1]);
				line = br.readLine();
				total += val;
				count++;
			}

		}

		// sort order based on RSSI
		Collections.sort(rssi_list);

		max = Integer.parseInt(rssi_list.get(0).split(",")[0]);
		min = Integer
				.parseInt(rssi_list.get(rssi_list.size() - 1).split(",")[0]);
		int threshold_RSSI = (max - min) / GRADENUM;
		int threshold_NUM = total / GRADENUM;

		// estimate grades
		// max = maximum, min = minimum, count = number of RSSI, total = number
		// of data
		Iterator<String> iter = rssi_list.iterator();
		ArrayList<String> finish = new ArrayList<String>();

		// new file for merging : final.txt
		Path pt2 = new Path("hdfs://" + host + ":9000" + RSSI + "final.txt");
		FileSystem fs2 = FileSystem.get(new Configuration());
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				fs2.create(pt2)));

		for (int i = 0; i < (GRADENUM - 1); i++) {
			String record = iter.next();
			int start = Integer.parseInt(record.split(",")[0]);
			int end;
			int data_num = 0;

			int threshold1 = start - threshold_RSSI;

			while (true) {
				int key = Integer.parseInt(record.split(",")[0]);
				int value = Integer.parseInt(record.split(",")[1]);

				data_num += value;

				// if reach the threshold
				if (key == threshold1 || data_num >= threshold_NUM) {
					end = key;
					break;
				}// end of if
				record = iter.next();
			}// end of while
			String result = String.valueOf(start) + "," + String.valueOf(end);
			finish.add(result + "," + String.valueOf(data_num));
			bw.write(result + "/");
		}
		bw.close();

		// third : last job for classification
		if (iteration == 1) {
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Wchecker");
			job.setJarByClass(Wchecker.class);
			job.setMapperClass(SecondMapper.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]
					+ "/classification_last"));
			
			job.waitForCompletion(true);

			iteration++;
		}

		Path pt3 = new Path("hdfs://" + host + ":9000" + OUT);
		FileSystem fs3 = FileSystem.get(new Configuration());
		BufferedWriter bwb = new BufferedWriter(new OutputStreamWriter(
				fs3.create(pt3)));

		bwb.write("[MAX] " + max + "\n");
		bwb.write("[MIN] " + min + "\n");
		bwb.write("[TOTAL] " + total + "\n");
		String last = "";
		int last_num = total;
		
		for ( int i=0; i<(GRADENUM-1); i++){
			String line = finish.get(i);
			String[] info = line.split(",");
			bwb.write("[GRADE" + (i+1) + "] " + info[0] + " ~ " + info[1] +", " + info[2] + "\n");
			last = info[1];
			int tmp = Integer.parseInt(info[2]);
			last_num -= tmp;
		}
		
		bwb.write("[GRADE" + GRADENUM + "] " + last + " ~ " + min +", " + last_num + "\n");
		
		bwb.close();
	}
}

