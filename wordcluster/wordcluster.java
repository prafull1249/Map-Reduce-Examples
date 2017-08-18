package wordcluster;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class wordcluster extends Configured implements Tool{

@Override
public int run(String[] args) throws Exception{

//Trivial check to see if the usage of input and output path arguments is correct
  if(args.length!=2){
    System.err.println("Invalid arguments!");
    System.err.println("Usage: <input path> <output path>");
    return -1;
  }

  //get the job instance and set its name
  Job job = Job.getInstance(getConf());
  job.setJobName("wordcluster");

  //Set the jar by class name of the existing driver class
  job.setJarByClass(wordcluster.class);

  //Set the output class of the key as the value type is supposed to be empty it is set as null
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(NullWritable.class);

  //Set the iput and output path as supplied by the CLI arguments.
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));

  //The mapper and reducer class is set
  job.setMapperClass(WordClusterMapper.class);
  job.setReducerClass(WordClusterReducer.class);
  //execute the job
  return job.waitForCompletion(true) ? 0:1;

}


  public static void main(String args[]) throws Exception{
    int exitCode = ToolRunner.run(new wordcluster(), args);
    System.exit(exitCode);
  }
}
