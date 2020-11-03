package nju;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
public class CommFriends {
	public static Set<String> intersect(Set<String> set1, Set<String> set2){
		if(set1==null || set2 == null){
			return null;
		}
		Set<String> result = new TreeSet<String>();
		Set<String> small = null;
		Set<String> big = null;
		if(set1.size() < set2.size()){
			small = set1;
			big = set2;
		}
		else {
			small = set2;
			big = set1;
		}
		
		for (String String : small) {
			if(big.contains(String)){
				result.add(String);
			}
		}
		return result;
	}
	
    static Set<String> all_users=new TreeSet<String>();
    static class MyMapper0 extends Mapper<LongWritable, Text, Text, Text>{
		
		private static Text outKey = new Text();
		private static Text outValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] input = value.toString().split(",");
			if(input.length != 2){
				return;
			}
            outValue.set(input[1]);
            outKey.set(input[0]);
            all_users.add(input[0]);
            context.write(outKey, outValue);
		}
	}
	
	static class MyReducer0 extends Reducer<Text, Text, Text, Text>{

		private static Text outKey = new Text();
		private static Text outValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			for (Text text : value) {
				outValue.set(text);
				outKey.set(key+",");
				context.write(outKey, outValue);
			}
		}
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		private static Text outKey = new Text();
		private static Text outValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] input = value.toString().split(",");
			if(input.length != 2){
				return;
			}
			outValue.set(input[1]);
			for (String string : all_users) {
				if(input[0].compareTo(string) < 0){
					outKey.set("[" + input[0] + ", " + string + "]");
				}
				else {
					outKey.set("[" + string + ", " + input[0] + "]");
				}
				context.write(outKey, outValue);
			}
		}
	}
	
	static class MyReducer extends Reducer<Text, Text, Text, Text>{

		private Text outValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			
			int len = 0;
			Set<String> set1 = new TreeSet<String>();
			Set<String> set2 = new TreeSet<String>();
			ArrayList<String> arrayList = new ArrayList<String>();
			for (Text text : value) {
				arrayList.add(text.toString());
				len++;
			}
			
			if(len != 2){
				return;
			}
			
			String [] sz = arrayList.get(0).split(" ");
			for (String s : sz) {
				set1.add(s);
			}
			
			sz = arrayList.get(1).trim().split(" ");
			for (String s : sz) {
				set2.add(s);
			}
			
			Set<String> res = intersect(set1, set2);
			if(res == null){
				return;
			}
			StringBuilder sb = new StringBuilder();
			for (String s : res) {
				sb.append(s + ", ");
			}
			
			String substring = null;
			if(sb.length() > 1){
				substring = sb.substring(0, sb.length()-2);
			}
			
			if(substring != null){
				this.outValue.set("["+substring+"]");
				context.write(new Text(key.toString()+","), outValue);
			}
		}
	}
	
	private static String inputPath = "hdfs://localhost:9000/user/sheepxi/input";
	private static String outputPath = "hdfs://localhost:9000/user/sheepxi/output";
	private static String outputPath2 = "hdfs://localhost:9000/user/sheepxi/output2";
 
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
		@SuppressWarnings("deprecation")
		Job job1 = new Job(conf, "all users");
		job1.setJarByClass(CommFriends.class);
		job1.setMapperClass(MyMapper0.class);
		job1.setReducerClass(MyReducer0.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileSystem fs1 = FileSystem.get(conf);
		Path inPath1 = new Path(inputPath);
		if (fs1.exists(inPath1)) {
			FileInputFormat.addInputPath(job1, inPath1);
		}
		Path outPath1 = new Path(outputPath);
		fs1.delete(outPath1, true);
		FileOutputFormat.setOutputPath(job1, outPath1);

        ControlledJob ctrlJob1 = new ControlledJob(conf);
        ctrlJob1.setJob(job1);
        @SuppressWarnings("deprecation")
		Job job2 = new Job(conf, "common friends");
		job2.setJarByClass(CommFriends.class);
		job2.setMapperClass(MyMapper.class);
		job2.setReducerClass(MyReducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		Path inPath2 = new Path(outputPath);
		FileInputFormat.addInputPath(job2, inPath2);
		Path outPath2 = new Path(outputPath2);
        FileOutputFormat.setOutputPath(job2, outPath2);
        //System.exit(job.waitForCompletion(true) ? 0 : 1);

        ControlledJob ctrlJob2 = new ControlledJob(conf);
		ctrlJob2.setJob(job2);
		ctrlJob2.addDependingJob(ctrlJob1);
        //设置主控制器，控制job1和job2两个作业
        JobControl jobCtrl = new JobControl("myCtrl");
        //添加到总的JobControl里，进行控制
        jobCtrl.addJob(ctrlJob1);
        jobCtrl.addJob(ctrlJob2);

		System.out.println("Job Start!");

		//在线程中启动
        Thread thread = new Thread(jobCtrl);
        thread.start();
        while (true) {
            if (jobCtrl.allFinished()) {
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }

    }
}