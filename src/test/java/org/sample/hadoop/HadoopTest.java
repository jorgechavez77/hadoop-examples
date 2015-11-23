package org.sample.hadoop;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.Test;

public class HadoopTest {

	private static final Logger LOG = Logger.getLogger(HadoopTest.class);

	@Test
	public void test01() throws IOException {
		URI uri = URI.create("hdfs://192.168.58.132:9000");

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(uri, conf);

		Path p1 = new Path("/jorge/test.txt");
		Path p2 = new Path("/user/root/test.txt");

		Path root = new Path("/user/root");

		RemoteIterator<?> it = fs.listFiles(root, true);
		while (it.hasNext()) {
			LOG.info(it.next());
		}

		fs.copyFromLocalFile(p1, p2);

		Path filePath = new Path("/home/test.txt");

		if (fs.exists(filePath)) {
			System.out.println("file exists: " + filePath);
		}
		if (fs.isFile(filePath)) {
			System.out.println("it is a file: " + filePath);
		}

		if (fs.createNewFile(filePath)) {
			System.out.println("file created: " + filePath);
		}

		// if (fs.delete(filePath, true)) {
		// System.out.println("4: " + filePath);
		// }

		FSDataInputStream in = fs.open(filePath);

		Path newPath = new Path("/user/root/new.txt");
		FSDataOutputStream out = fs.create(newPath);

		String data = "More data on the test file";
		IOUtils.write(data, out);

		LOG.info(IOUtils.toString(in));

		fs.close();
	}

	@Test
	public void test02() throws IOException {
		Configuration conf = new Configuration();
		Path path = new Path("/user/root/sequenceFileSample.txt");

		conf.set("fs.defaultFS", "hdfs://192.168.58.132:9000");

		SequenceFile.Writer sequenceWriter = SequenceFile.createWriter(conf,
				Writer.file(path), Writer.keyClass(Text.class),
				Writer.valueClass(IntWritable.class));

		sequenceWriter.append(new Text("key1"), new IntWritable(1));
		sequenceWriter.append(new Text("key2"), new IntWritable(2));

		sequenceWriter.close();

		SequenceFile.Reader sequenceReader = new Reader(conf, Reader.file(path));

		Text key = new Text();
		IntWritable val = new IntWritable();

		while (sequenceReader.next(key, val)) {
			LOG.info(key + "\t" + val);
		}
		sequenceReader.close();
	}

	@Test
	public void test03() throws IOException {
		Configuration conf = new Configuration();
		Path path = new Path("/user/root/sequenceFileSample.txt");

		conf.set("fs.defaultFS", "hdfs://192.168.58.132:9000");

		SequenceFile.Writer sequenceWriter = SequenceFile.createWriter(conf,
				Writer.file(path), Writer.keyClass(Text.class),
				Writer.valueClass(Text.class));

		sequenceWriter.append(new Text("key1"), new Text(
				"Some text here for key1!"));
		sequenceWriter.append(new Text("key2"), new Text(
				"Some text here for key2!"));
		sequenceWriter.append(new Text("key3"), new Text(
				"Some text here for key3!"));

		sequenceWriter.close();

		SequenceFile.Reader sequenceReader = new Reader(conf, Reader.file(path));

		Text key = new Text();
		Text val = new Text();

		while (sequenceReader.next(key, val)) {
			LOG.info(key + "\t" + val);
		}
		sequenceReader.close();
	}

	@Test
	public void test04() throws IOException {
		
		Configuration.addDefaultResource("conf/hbase-site.xml");
		Configuration conf = HBaseConfiguration.create(new Configuration());
		
//		conf.set("hbase.master", "192.168.58.132:16000");
//		conf.set("hbase.zookeeper.quorum", "192.168.58.132");
		Connection connection = ConnectionFactory.createConnection(conf);

		Table table = connection.getTable(TableName.valueOf("test"));
		LOG.info(table.getName());
		Scan scan = new Scan();

		ResultScanner scanner = table.getScanner(scan);

		Result result = null;
		while ((result = scanner.next()) != null) {
			LOG.info(result);
		}

		connection.close();

		// Admin admin = connection.getAdmin();
		// TableName tableName = TableName.valueOf("test");
	}
}
