package Houses;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.text.DecimalFormat;


public class HousesReducer  extends Reducer <Text,IntWritable,Text,FloatWritable> 
{

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	{
		
		float min=9999999.99f, max=0.0f, count=0.0f,sum=0.0f;
		float mean=0.0f;
		FloatWritable result = new FloatWritable();


		for(IntWritable price:values)
		{
			sum=(float)(sum+price.get());
			count++;

			if(min>price.get())
			{
				min=(float)price.get();

			}
			if(max<price.get())
			{
				max=(float)price.get();
			}
		}
		
		result.set(min);
		context.write(key, result);

		mean= sum/(float)count;
		result.set(mean);
		context.write(key, result);

		result.set(max);
		context.write(key, result);


   	}
}
