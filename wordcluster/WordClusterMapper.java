package wordcluster;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.StringBuilder;

public class WordClusterMapper extends Mapper<LongWritable,Text,Text,Text> {

private Text keyWord = new Text();
private Text valueWord = new Text();
@Override
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
    /*
      IT is assumed that the text sequence input file provides us the key as the line number
      and the value as the line string. Hence, that explains the LongWritable type for key and Text value
      type for the value which is supplied to the mapper class here.
    */

    //Get the value as string to process it.
    String line = value.toString().trim();
    //Get rid of foreign characters as it is assumed only characters a-z, A-Z,
    //whitespaces and 0-9 are considered in the text
    //MAke the entire string as lower case and split it to get each word.
    String[] words = line.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase().split("\\s+");

    for(String word: words){
      // Skip the word if it is blank
      if(word.length()==0) continue;

      //maintain a map of ascii characters which will help us to sort the characters in the word.
      int[] map = new int[255];
      //for every character in the word increment the count of the character
      for(int i=0;i<word.length();i++){
        char c = word.charAt(i);
        map[c]++;
      }
      //Stringbuilder for memory efficient string processing.
      StringBuilder sb = new StringBuilder();

      //The map is used to iterate in ascending order of the characters which are appended in the
      //stringbuilder to sort them. O(n) complexity.
      for(int i=0;i<255;i++){
        //Iterate over all the 255 characters in the map for current "word"
        while(map[i]>0){
          sb.append(Character.toChars(i));
          map[i]--;
        }
      }
      //Emit the value of sb.toString as the key and String value of the word as its value
      //ex. the words "top" and "pot" will have the same key "opt" which will emit the values <"opt","top"> & <"opt","pot">
      keyWord.set(sb.toString());
      valueWord.set(word);
      context.write(keyWord, valueWord);
    }
  }
}
