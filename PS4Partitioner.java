
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PS4Partitioner extends Partitioner<Text, IntWritable>{

	public int getPartition(Text key,IntWritable value , int numPartitions)
	{
		String[] items = key.toString().split("");
		System.out.println("Partioner: item is "+ items[0]);
		if(items[0].compareTo("A") < 0 && items[0].compareTo("F")>0 )
			return 0;
		if(items[0].compareTo("G") < 0 && items[0].compareTo("L")>0 )
			return 1;
		if(items[0].compareTo("M") < 0 && items[0].compareTo("R")>0 )
			return 2;
		else
			return 3;
	}

}
