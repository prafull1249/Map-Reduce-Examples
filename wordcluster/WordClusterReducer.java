package wordcluster;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;
import java.lang.StringBuilder;

public class WordClusterReducer extends Reducer<Text,Text,Text,NullWritable> {


@Override
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
      //StringBuilder for memory efficient string processing.
      StringBuilder sb = new StringBuilder();
      Set<String> set = new HashSet<>();
      //length of the stringbuilder to get rid of the last appended ","
      int len = 0;
      //Iterate over the values with the same sorted key and concatenate them with ',' as the delimiter
      for(Text word: values){
          if(set.contains(word.toString())) continue;
          else set.add(word.toString());

          sb.append(word.toString());
          len = sb.length();
          sb.append(", ");
      }

      sb.setLength(len);
      //As the value is supposed to be empty we jsut emit the key and emit zero length nullwritable value
      context.write(new Text(sb.toString()), NullWritable.get());
    }
  }
