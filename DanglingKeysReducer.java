package danglingkeyssolution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

public static class DanglingKeysReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException {

			int totaloccurrence = 0;

			for(IntWritable value: values)
			{
				//Summing up the totaloccurrence to check if 1 appears in the list of values emitted by mappers.
				totaloccurrence+=value.get();
			}

			/*
			If the value is not present in the Key set of the files, then its total
			summed value of occurrence should be more than 0.
			As we have emitted 0 for values which are in valueset of a given sequence file,
			its total sum should be 0 for a particular value if it does not ever appear in the keySet of the input.
			*/

			if(totaloccurrence==0)
				context.write(key, new IntWritable(totaloccurrence));
		}
	}
