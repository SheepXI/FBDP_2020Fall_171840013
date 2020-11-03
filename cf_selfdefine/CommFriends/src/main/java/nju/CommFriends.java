package nju;

import java.io.DataInput;
import java.io.DataOutput;
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
import org.apache.hadoop.io.WritableComparable;


class Friends implements WritableComparable<Friends>{
	private static String person1 = new String();
	private static String person2 = new String();
	private static String commfriend = new String();

	public Friends(String str1,String str2,String str3){
		this.person1=str1;
		this.person2=str2;
		this.commfriend=str3;
	}

	@Override
	public void readFields(DataInput in) throws IOException{
		person1=in.readUTF();
		person2=in.readUTF();
		commfriend=in.readUTF();
	}
	@Override
	public void write(DataOutput out) throws IOException{
		out.writeUTF("["+person1+","+person2+"]");
		out.writeUTF(":");
		out.writeUTF(commfriend);
	}
	@Override
	public int compareTo(Friends o){
		return commfriend.compareTo(o.commfriend);
	}
	@Override
	public String toString() {
		return "["+person1+","+person2+"]"+":"+commfriend;
	}
}
 
public class CommFriends {
	
	private static Set<Text> all_users=new TreeSet<Text>();
	
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
			all_users.add(new Text(input[0]));
			for (String str:input[1].toString().split(" ")){
				outKey.set(new Text(str));
				outValue.set(input[0]);
				context.write(outKey, outValue); //key is one of value's friends
			}
		}
	}
	
	static class MyReducer0 extends Reducer<Text, Text, Text, Text>{

		private static Text outKey = new Text();
		private static Text outValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			Set<String> s=new TreeSet<String>();
			for (Text text : value){
				s.add(text.toString());
			}
			for (String str1:s) {
				for (String str2:s){
					//String str1=new String(text1.toString());
					//String str2=new String(text2.toString());
					System.out.println(str1+","+str2);
					if (str1.compareTo(str2)<0){
						Friends temp = new Friends(str1,str2,key.toString());
						System.out.println(temp.toString());
						outKey.set(key+":");
						outValue.set(temp.toString());
						context.write(outKey,outValue);
					}
				}
			}
		}
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		private static Text outKey = new Text();
		private static Text outValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] input = value.toString().split(":");
			if(input.length != 3){
				return;
			}
			String pattern = "\t";
			outKey.set((input[1]+",").replaceAll(pattern,""));
			outValue.set(input[2].replaceAll(pattern,""));
			context.write(outKey, outValue);
		}
	}
	
	static class MyReducer extends Reducer<Text, Text, Text, Text>{

		private Text outValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			String str=new String("[");
			for (Text text : value){
				str+=text.toString()+",";
			}
			str=str.substring(0,str.length()-1);
			str+="]";
			context.write(key,new Text(str));
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