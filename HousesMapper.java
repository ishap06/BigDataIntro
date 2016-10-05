package Houses;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.io.BufferedReader;
import java.io.FileReader;



import java.io.IOException;
import java.util.StringTokenizer;

public class HousesMapper  extends Mapper <LongWritable,Text,Text,IntWritable> 
{
	Log log = LogFactory.getLog(HousesMapper.class);
	static int record_count=0;
	Text neighborhood;
	IntWritable price;

   	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	{
		record_count++;
		
		if(record_count==1)
		{
			return;
		}
		
		else
		{
			String line= value.toString();
			String record[]=line.split(",");
			
			if(line.length()<81)
			{
				log.error("This row contains missing elements");
			}
			if(record[12].equals(""))
			{
				log.error("This row contains null values");
			}
			
			if(Integer.parseInt(record[80]) <0)
			{
				log.error("The price is invalid.");
			}
			
			neighborhood= new Text (record[12]);
			price= new IntWritable(Integer.parseInt(record[80]));
			context.write(neighborhood, price);
			
		}
   	}

}
