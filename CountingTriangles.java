import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.lang.*;
import java.lang.Math;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CountingTriangles {

  public static class CountTriangleMapper1 extends Mapper<Object, Text, Text, IntWritable>{

    private Text node1 = new Text();
    private Text node2 = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        node1.set(itr.nextToken());
        node2.set(itr.nextToken());

        Text nodeKey = new Text(Integer.toString(Math.min(Integer.parseInt(node1.toString()), Integer.parseInt(node2.toString()))));
        IntWritable edge = new IntWritable(Math.max(Integer.parseInt(node1.toString()), Integer.parseInt(node2.toString())));

        context.write(nodeKey, edge);
      }
    }
  }

  public static class CountTriangleReducer1 extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      List<Integer> valueList = new ArrayList<Integer>(); 
      for (IntWritable val : values) {
        valueList.add(val.get());
      }
      
      for (int i = 0; i < valueList.size(); i++) {
        for (int j = 0; j < valueList.size(); j++) {
          int val1 = valueList.get(i);
          int val2 = valueList.get(j);
          int[] valuesArray = {val1, val2};
          Arrays.sort(valuesArray);
          Text output = new Text(Integer.toString(val1)+","+Integer.toString(val2));
          IntWritable outputKey = new IntWritable(Integer.parseInt(key.toString()));
          context.write(output, outputKey);
        }
      }
      context.write(key, result);
    }
  }

  public static class CountTriangleMapper2 extends Mapper<LongWritable, Text, Text, IntWritable>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String valueString = value.toString();
      String[] edge = valueString.split("\\s+");

      if (edge.length > 1) {
        Text nodeKey = new Text(edge[0]); 
        IntWritable val = new IntWritable(1);
        context.write(nodeKey, val);

      } else {
        String[] nodeKeys = edge[0].split(",");

        int nodeKey1 = Integer.parseInt(nodeKeys[0]);
        int nodeKey2 = Integer.parseInt(nodeKeys[1]);

        Text nodeKey = new Text(Integer.toString(Math.min(nodeKey1,nodeKey2)) +","+ Integer.toString(Math.max(nodeKey1,nodeKey2)));

        IntWritable val = new IntWritable(0);
        context.write(nodeKey, val);
      }
    }
  }

  public static class CountTriangleReducer2 extends Reducer<Text,IntWritable,Text,IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int count0 = 0;
      int count1 = 0;
      for (IntWritable value : values){
        if (value.get() == 0) {
          count0++;
        } else {
          count1++;
        }
      }

      IntWritable result = new IntWritable(count0 > 0 ? count1 : 0);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "#2019GantiPresiden...KM");
    job1.setJarByClass(CountingTriangles.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/1"));
    job1.setMapperClass(CountTriangleMapper1.class);
    job1.setReducerClass(CountTriangleReducer1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);

    job1.waitForCompletion(true);

    Job job2 = Job.getInstance(conf, "#EdyOut");
    job2.setJarByClass(CountingTriangles.class);
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileInputFormat.addInputPath(job2,	new Path(args[1]+"/1"));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]+"/2"));
    job2.setMapperClass(CountTriangleMapper2.class);
    job2.setReducerClass(CountTriangleReducer2.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}