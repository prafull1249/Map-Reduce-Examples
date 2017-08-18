package danglingkeyssolution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import DanglingKeysMapper;
import DanglingKeysReducer;

/**
 * @author Prafull Gaikwad
 *
 */

public class DanglingKeys extends Configured implements Tool {

 @Override
 public int run(String[] args) throws Exception {
  /*
   * Run function overrided to write the driver code for the mappers and reducers.
   */

  Configuration conf = getConf();
  FileSystem fs = FileSystem.get(conf);

  Job job = new Job(conf, "FindDanglingKeys");
  job.setJarByClass(DanglingKeys.class);

  //Set input and output file paths which are supplied at the CLI while running the hadoop program.
  TextInputFormat.addInputPath(job, new Path(args[0]));
  TextOutputFormat.setOutputPath(job, new Path(args[1]));

  //Set the input files formats for the job.
  /*Input file is set as SequenceFileInputFormat which is assumed
   to automatically read the key and value by RecordReader when mapper is run.*/
  job.setInputFormatClass(SequenceFileInputFormat.class);
  job.setOutputFormatClass(TextOutputFormat.class);

  //Set the mapper and reducer classes.
  job.setMapperClass(DanglingKeysMapper.class);
  job.setReducerClass(DanglingKeysReducer.class);

  //Set output formats for key and value.
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);

  return job.waitForCompletion(true) ? 0 : 1;
 }

 public static void main(String[] args) throws Exception {

  if (args.length != 2) {
   System.err.println("Enter valid number of arguments <inputDir> <outputDir>");
   System.exit(0);
  }
  //Run the driver code
  ToolRunner.run(new Configuration(), new DanglingKeys(), args);
 }
}
