package danglingkeyssolution;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

public static class DanglingKeysMapper extends Mapper<Text,Text,Text,IntWritable>{

		private final static IntWritable koccurrence = new IntWritable(1);
		private final static IntWritable voccurrence = new IntWritable(0);

		public void map(Text key, Text value,	Context context) throws IOException,InterruptedException {

			String record = value.toString();
      // The Values list is separated by tabs and hence the string is split to get individual values.
			String str[] = record.split("\t");

			//Preparing the key Text object for Output
			Text newk = new Text();
			newk.set(key.toString());

			//As koccurrence is 1, we emit key newk with value as 1.
			context.write(newk, koccurrence);

			if(str.length>0){
				for(int i=0;i<str.length;i++){
					Text val = new Text();

					//If length of any value is 0, we just ignore it.
					if(str[i].length==0)
						continue;

					val.set(str[i]);
					//As voccurrence is 0, we emit key "val" with value as 0.
					context.write(val, voccurrence);
				}
			}

		}
	}
