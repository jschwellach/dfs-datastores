package com.backtype.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.backtype.hadoop.formats.RecordInputStream;
import com.backtype.hadoop.formats.RecordOutputStream;
import com.backtype.hadoop.formats.RecordStreamFactory;
import com.backtype.support.SubsetSum;
import com.backtype.support.SubsetSum.Value;
import com.backtype.support.Utils;

import scala.Tuple2;

public class SparkConsolidator {
	public static final long DEFAULT_CONSOLIDATION_SIZE = 1024 * 1024 * 127; // 127
																				// MB
	private static final String ARGS = "consolidator_args";

	private static final Logger LOG = LoggerFactory.getLogger(SparkConsolidator.class);
	private static Thread shutdownHook;
	private static RunningJob job = null;

	public static class ConsolidatorArgs implements Serializable {
		private static final long serialVersionUID = 2950191562489044459L;
		public String fsUri;
		public RecordStreamFactory streams;
		public PathLister pathLister;
		public List<String> dirs;
		public long targetSizeBytes;
		public String extension;

		public ConsolidatorArgs(String fsUri, RecordStreamFactory streams, PathLister pathLister, List<String> dirs,
				long targetSizeBytes, String extension) {
			this.fsUri = fsUri;
			this.streams = streams;
			this.pathLister = pathLister;
			this.dirs = dirs;
			this.targetSizeBytes = targetSizeBytes;
			this.extension = extension;
		}
	}

	public static void consolidate(FileSystem fs, String targetDir, RecordStreamFactory streams, PathLister pathLister,
			long targetSizeBytes) throws IOException {
		consolidate(fs, targetDir, streams, pathLister, targetSizeBytes, "");
	}

	public static void consolidate(FileSystem fs, String targetDir, RecordStreamFactory streams, PathLister pathLister,
			String extension) throws IOException {
		consolidate(fs, targetDir, streams, pathLister, DEFAULT_CONSOLIDATION_SIZE, extension);
	}

	public static void consolidate(FileSystem fs, String targetDir, RecordStreamFactory streams, PathLister pathLister)
			throws IOException {
		consolidate(fs, targetDir, streams, pathLister, DEFAULT_CONSOLIDATION_SIZE, "");
	}

	public static void consolidate(FileSystem fs, String targetDir, RecordStreamFactory streams, PathLister pathLister,
			long targetSizeBytes, String extension) throws IOException {
		List<String> dirs = new ArrayList<String>();
		dirs.add(targetDir);
		consolidate(fs, streams, pathLister, dirs, targetSizeBytes, extension);
	}

	private static String getDirsString(List<String> targetDirs) {
		String ret = "";
		for (int i = 0; i < 3 && i < targetDirs.size(); i++) {
			ret += (targetDirs.get(i) + " ");
		}
		if (targetDirs.size() > 3) {
			ret += "...";
		}
		return ret;
	}

	public static void consolidate(FileSystem fs, RecordStreamFactory streams, PathLister lister, List<String> dirs,
			long targetSizeBytes, String extension) throws IOException {

		String fsUri = fs.getUri().toString();
		ConsolidatorArgs args = new ConsolidatorArgs(fsUri, streams, lister, dirs, targetSizeBytes, extension);
		SparkConf conf = new SparkConf();

		// we need to add the following two lines to be able to serialize the
		// java classes of hadoop. This list is comma seperated
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
				.set("spark.kryo.classesToRegister", "org.apache.hadoop.io.Text")
				.set("spark.kryo.classesToRegister", "org.apache.hadoop.io.ArrayWritable")
				.set("spark.kryo.classesToRegister", "org.apache.hadoop.fs.LocalFileSystem")
				.set("spark.kryoserializer.buffer.max value", "2g");

		SparkContext sc = new SparkContext(conf.setMaster("local").setAppName("Consolidator: " + getDirsString(dirs)));
		JavaSparkContext jsc = new JavaSparkContext(sc);
		Utils.setObject(jsc.hadoopConfiguration(), ARGS, args);

		for (String dir : dirs) {
			JavaPairRDD<ArrayWritable, Text> hadoopFile = jsc.hadoopFile(dir, ConsolidatorInputFormat.class,
					ArrayWritable.class, Text.class, 0);
			LOG.debug("executing {}",dir);
			
			
			ConsolidatorFunction function = new ConsolidatorFunction(args);
			function.configure(jsc.hadoopConfiguration());
			
			hadoopFile.foreach(function);
			
		}

	}

	private static void registerShutdownHook() {
		shutdownHook = new Thread() {
			@Override
			public void run() {
				try {
					if (job != null)
						job.killJob();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		};
		Runtime.getRuntime().addShutdownHook(shutdownHook);
	}

	private static void deregisterShutdownHook() {
		Runtime.getRuntime().removeShutdownHook(shutdownHook);
	}
	
	public static class ConsolidatorFunction implements VoidFunction<Tuple2<ArrayWritable,Text>> {
		private static final long serialVersionUID = -5690741052298124421L;
		private transient FileSystem fs;
		private ConsolidatorArgs args;
		
		public ConsolidatorFunction(ConsolidatorArgs args) {
			this.args = args;
		}
		
		@Override
		public void call(Tuple2<ArrayWritable, Text> t) throws Exception {
			try {
				fs = Utils.getFS(args.fsUri);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
			ArrayWritable sourcesArr = t._1;
			Text target = t._2();
			Path finalFile = new Path(target.toString());
			
			List<Path> sources = new ArrayList<Path>();
			for (int i = 0; i < sourcesArr.get().length; i++) {
				sources.add(new Path(((Text) sourcesArr.get()[i]).toString()));
			}
			// must have failed after succeeding to create file but before task
			// finished - this is valid
			// because path is selected with a UUID
			if (!fs.exists(finalFile)) {
				Path tmpFile = new Path("/tmp/consolidator/" + UUID.randomUUID().toString());
				fs.mkdirs(tmpFile.getParent());

				String status = "Consolidating " + sources.size() + " files into " + tmpFile.toString();
				LOG.info(status);

				RecordStreamFactory fact = args.streams;
				fs.mkdirs(finalFile.getParent());

				RecordOutputStream os = fact.getOutputStream(fs, tmpFile);
				for (Path i : sources) {
					LOG.info("Opening " + i.toString() + " for consolidation");
					RecordInputStream is = fact.getInputStream(fs, i);
					byte[] record;
					while ((record = is.readRawRecord()) != null) {
						os.writeRaw(record);
					}
					is.close();
				}
				os.close();

				status = "Renaming " + tmpFile.toString() + " to " + finalFile.toString();
				LOG.info(status);

				if (!fs.rename(tmpFile, finalFile))
					throw new IOException("could not rename " + tmpFile.toString() + " to " + finalFile.toString());
			}

			String status = "Deleting " + sources.size() + " original files";
			LOG.info(status);

			for (Path p : sources) {
				fs.delete(p, false);
			}
			
		}
		
		public void configure(Configuration conf) {
			args = (ConsolidatorArgs) Utils.getObject(conf, ARGS);
			try {
				fs = Utils.getFS(args.fsUri, conf);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
	}

	public static class ConsolidatorSplit implements InputSplit {
		public String[] sources;
		public String target;

		public ConsolidatorSplit() {

		}

		public ConsolidatorSplit(String[] sources, String target) {
			this.sources = sources;
			this.target = target;
		}

		public long getLength() throws IOException {
			return 1;
		}

		public String[] getLocations() throws IOException {
			return new String[] {};
		}

		public void write(DataOutput d) throws IOException {
			WritableUtils.writeString(d, target);
			WritableUtils.writeStringArray(d, sources);
		}

		public void readFields(DataInput di) throws IOException {
			target = WritableUtils.readString(di);
			sources = WritableUtils.readStringArray(di);
		}

	}

	public static class ConsolidatorRecordReader implements RecordReader<ArrayWritable, Text> {
		private ConsolidatorSplit split;
		boolean finished = false;

		public ConsolidatorRecordReader(ConsolidatorSplit split) {
			this.split = split;
		}

		public boolean next(ArrayWritable k, Text v) throws IOException {
			if (finished)
				return false;
			Writable[] sources = new Writable[split.sources.length];
			for (int i = 0; i < sources.length; i++) {
				sources[i] = new Text(split.sources[i]);
			}
			k.set(sources);
			v.set(split.target);

			finished = true;
			return true;
		}

		public ArrayWritable createKey() {
			return new ArrayWritable(Text.class);
		}

		public Text createValue() {
			return new Text();
		}

		public long getPos() throws IOException {
			if (finished)
				return 1;
			else
				return 0;
		}

		public void close() throws IOException {
		}

		public float getProgress() throws IOException {
			if (finished)
				return 1;
			else
				return 0;
		}

	}

	public static class ConsolidatorInputFormat implements InputFormat<ArrayWritable, Text> {

		private static class PathSizePair implements Value {
			public Path path;
			public long size;

			public PathSizePair(Path p, long s) {
				this.path = p;
				this.size = s;
			}

			public long getValue() {
				return size;
			}

		}

		private List<PathSizePair> getFileSizePairs(FileSystem fs, List<Path> files) throws IOException {
			List<PathSizePair> results = new ArrayList<PathSizePair>();
			for (Path p : files) {
				long size = fs.getContentSummary(p).getLength();
				results.add(new PathSizePair(p, size));
			}
			return results;
		}

		private String[] pathsToStrs(List<PathSizePair> pairs) {
			String[] ret = new String[pairs.size()];
			for (int i = 0; i < pairs.size(); i++) {
				ret[i] = pairs.get(i).path.toString();
			}
			return ret;
		}

		private List<InputSplit> createSplits(FileSystem fs, List<Path> files, String target, long targetSize,
				String extension) throws IOException {
			List<PathSizePair> working = getFileSizePairs(fs, files);
			List<InputSplit> ret = new ArrayList<InputSplit>();
			List<List<PathSizePair>> splits = SubsetSum.split(working, targetSize);
			for (List<PathSizePair> c : splits) {
				if (c.size() > 1) {
					String rand = UUID.randomUUID().toString();
					String targetFile = new Path(target,
							"" + rand.charAt(0) + rand.charAt(1) + "/cons" + rand + extension).toString();
					ret.add(new ConsolidatorSplit(pathsToStrs(c), targetFile));

				}
			}
			Collections.sort(ret, new Comparator<InputSplit>() {
				public int compare(InputSplit o1, InputSplit o2) {
					return ((ConsolidatorSplit) o2).sources.length - ((ConsolidatorSplit) o1).sources.length;
				}
			});
			return ret;
		}

		public InputSplit[] getSplits(JobConf conf, int ignored) throws IOException {
			ConsolidatorArgs args = (ConsolidatorArgs) Utils.getObject(conf, ARGS);
			PathLister lister = args.pathLister;
			List<String> dirs = args.dirs;
			List<InputSplit> ret = new ArrayList<InputSplit>();
			for (String dir : dirs) {
				FileSystem fs = Utils.getFS(dir, conf);
				ret.addAll(createSplits(fs, lister.getFiles(fs, dir), dir, args.targetSizeBytes, args.extension));
			}
			return ret.toArray(new InputSplit[ret.size()]);
		}

		public RecordReader<ArrayWritable, Text> getRecordReader(InputSplit is, JobConf jc, Reporter rprtr)
				throws IOException {
			return new ConsolidatorRecordReader((ConsolidatorSplit) is);
		}
	}

}
