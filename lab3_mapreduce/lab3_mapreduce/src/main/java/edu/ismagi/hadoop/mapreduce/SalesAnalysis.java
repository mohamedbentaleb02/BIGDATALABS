package edu.ismagi.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesAnalysis {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sales Analysis");
        
        job.setJarByClass(SalesAnalysis.class);
        
        job.setMapperClass(SalesMapper.class);
        job.setCombinerClass(SalesReducer.class); 
        job.setReducerClass(SalesReducer.class);
        
        job.setOutputKeyClass(Text.class);
        // ATTENTION : On utilise DoubleWritable ici pour les prix
        job.setOutputValueClass(DoubleWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}