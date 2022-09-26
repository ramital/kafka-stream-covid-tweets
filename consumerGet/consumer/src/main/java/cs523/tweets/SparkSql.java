package cs523.tweets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.json.JSONArray;

public class SparkSql {
	private static final String TABLE_NAME = "covid";
	private static final String CF_DEFAULT = "tweet-info";
	private static final String CF_GENERAL = "general-info";
	
	static Configuration config;
	static JavaSparkContext jsc;
	
	public static void main(String[] args) 
	{
		SparkConf sconf = new SparkConf().setAppName("SparkSQL").setMaster("local[3]");
		sconf.registerKryoClasses(new Class[] { org.apache.hadoop.hbase.io.ImmutableBytesWritable.class });
		
		config = HBaseConfiguration.create();
		config.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);

		jsc = new JavaSparkContext(sconf);
		SQLContext sqlContext = new SQLContext(jsc.sc());
		
		//Create Hbase RDD
		JavaPairRDD<ImmutableBytesWritable, Result> HBRDD = readTableByJavaPairRDD();
		System.out.println("# of records in hbase table: " + HBRDD.count());
		
		JavaRDD<Tweet> rows = HBRDD.map(x -> {
			Tweet tweet = new Tweet();
			
			tweet.setId(Bytes.toString(x._1.get()));
			tweet.setText(Bytes.toString(x._2.getValue(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("text")))); 
			tweet.setInReplyToStatusId(Bytes.toString(x._2.getValue(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("inReplyToStatusId")))); 
			tweet.setUsername(Bytes.toString(x._2.getValue(Bytes.toBytes(CF_GENERAL), Bytes.toBytes("username"))));
			tweet.setTimeStamp(Bytes.toString(x._2.getValue(Bytes.toBytes(CF_GENERAL), Bytes.toBytes("time_stamp"))));
			tweet.setLang(Bytes.toString(x._2.getValue(Bytes.toBytes(CF_GENERAL), Bytes.toBytes("lang"))));

			return tweet;
		});

		DataFrame table = sqlContext.createDataFrame(rows, Tweet.class);
		table.registerTempTable(TABLE_NAME);
		table.printSchema();

		//queries
		DataFrame q1 = sqlContext.sql("Select username, count(*) from covid group by username ORDER BY count(*) desc limit 10");
		q1.show();

		DataFrame q2 = sqlContext.sql("Select inReplyToStatusId, count(*) from covid GROUP BY inReplyToStatusId ORDER BY count(*) desc");
		q2.show();		 

	    DataFrame query3 = sqlContext.sql("Select count(*), lang from covid GROUP BY lang ORDER BY count(*) desc");
	    query3.show();	
		 
		jsc.stop();

	}
	
    public static JavaPairRDD<ImmutableBytesWritable, Result> readTableByJavaPairRDD()
    {
    	JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD( config, TableInputFormat.class, org.apache.hadoop.hbase.io.ImmutableBytesWritable.class, org.apache.hadoop.hbase.client.Result.class);
    	return hBaseRDD;
    }
	

}
