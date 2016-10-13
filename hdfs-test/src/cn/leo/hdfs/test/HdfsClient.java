package cn.leo.hdfs.test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HdfsClient {
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		downloadTest();
	}

	public static void uploadTest() throws IOException, InterruptedException, URISyntaxException {
		FileSystem fs = getFileSystem();

//		FSDataOutputStream out = fs.create(new Path("hdfs://hadoop01:9000/hehe.txt"));
//		FileInputStream in = new FileInputStream("/root/anaconda-ks.cfg");
//		IOUtils.copy(in, out);
		
		fs.copyFromLocalFile(new Path("E:/hadoop/join/a.txt"), new Path("/join/srcdata/a.txt"));
	}

	public static void downloadTest() throws IOException, InterruptedException, URISyntaxException {
		FileSystem fs = getFileSystem();

//		FSDataInputStream in = fs.open(new Path("hdfs://hadoop01:9000/hehe.txt"));
//		FileOutputStream out = new FileOutputStream("D:/hehe.txt.download");
//		IOUtils.copy(in, out);
		fs.copyToLocalFile(new Path("/wc/heihei.txt"), new Path("D:/hehe.download.txt"));
		
	}

	public static void rm() throws IOException, InterruptedException, URISyntaxException{
		FileSystem fs = getFileSystem();
		fs.delete(new Path("/hehe.txt"), true);
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	public static FileSystem getFileSystem() throws IOException, InterruptedException, URISyntaxException {
		FileSystem fs = null;

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop05:9000/");
		//conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("dfs.replication", "1");

//		fs = FileSystem.get(conf);
		fs = FileSystem.get(new URI("hdfs://hadoop05:9000/"), conf, "root");

		return fs;
	}
	
	
	
	
	

}
