package com.backtype.hadoop;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.backtype.hadoop.FileCopyInputFormat.FileCopyArgs;
import com.backtype.hadoop.formats.RecordInputStream;
import com.backtype.hadoop.formats.RecordOutputStream;
import com.backtype.hadoop.formats.RecordStreamFactory;
import com.backtype.support.Utils;

public class SparkCoercer {
    private static final String FACTIN_ARG = "coercer_stream_factin_arg";
    private static final String FACTOUT_ARG = "coercer_stream_factout_arg";

    private static Thread shutdownHook;
    private static RunningJob job = null;

    public static void coerce(String source, String dest, int renameMode, PathLister lister, RecordStreamFactory factin,
            RecordStreamFactory factout) throws IOException {
        coerce(source, dest, renameMode, lister, factin, factout, "");
    }

    public static void coerce(String qualSource, String qualDest, int renameMode, PathLister lister,
            RecordStreamFactory factin, RecordStreamFactory factout, String extensionOnRename) throws IOException {
        coerce(qualSource, qualDest, renameMode, lister, factin, factout, extensionOnRename, new Configuration());
    }

    public static void coerce(String qualSource, String qualDest, int renameMode, PathLister lister,
            RecordStreamFactory factin, RecordStreamFactory factout, String extensionOnRename,
            Configuration configuration) throws IOException {
        if (!Utils.hasScheme(qualSource) || !Utils.hasScheme(qualDest))
            throw new IllegalArgumentException("source and dest must have schemes " + qualSource + " " + qualDest);

        FileCopyArgs args = new FileCopyArgs(qualSource, qualDest, renameMode, lister, extensionOnRename);

        SparkConf conf = new SparkConf();
        // we need to add the following two lines to be able to serialize the
        // java classes of hadoop. This list is comma seperated
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.classesToRegister", "org.apache.hadoop.io.Text")
                .set("spark.kryoserializer.buffer.max value", "2g");

        SparkContext sc = SparkContext
                .getOrCreate(conf.setMaster("local").setAppName("Coercer: " + qualSource + " -> " + qualDest));
        JavaSparkContext jsc = new JavaSparkContext(sc);

        Utils.setObject(jsc.hadoopConfiguration(), FileCopyInputFormat.ARGS, args);

        JavaPairRDD<Text, Text> hadoopFile = jsc.hadoopFile(args.source, FileCopyInputFormat.class, Text.class,
                Text.class);

        // FIXME: put this into a configuration
        String tmpRoot;
        try {
            tmpRoot = conf.get("spark.local.dir") != null
                    ? conf.get("spark.local.dir")
                    : System.getProperty("java.io.tmpdir");
        } catch (NoSuchElementException e) {
            tmpRoot = System.getProperty("java.io.tmpdir");
        }

        CoercerFunction coercerFunction = new CoercerFunction(args, tmpRoot, factin, factout);
        hadoopFile.foreach(coercerFunction);

        jsc.close();

    }

    public static class CoercerFunction extends AbstractFileCopyFunction {

        private static final long serialVersionUID = 4752638608073234028L;

        RecordStreamFactory factin;
        RecordStreamFactory factout;

        public CoercerFunction(FileCopyArgs args, String tmpRoot, RecordStreamFactory factin,
                RecordStreamFactory factout) {
            super(args, tmpRoot);
            this.factin = factin;
            this.factout = factout;
        }

        @Override
        protected void copyFile(FileSystem fsSource, Path source, FileSystem fsDest, Path target) throws IOException {
            RecordInputStream fin = factin.getInputStream(fsSource, source);
            RecordOutputStream fout = factout.getOutputStream(fsDest, target);

            try {
                byte[] record;
                int bytes = 0;
                while ((record = fin.readRawRecord()) != null) {
                    fout.writeRaw(record);
                    bytes += record.length;
                    if (bytes >= 1000000) { // every 1 MB of data report
                                            // progress so we don't time out on
                                            // large files
                        bytes = 0;
                        // FIXME: is there a reporter in spark?
                        // reporter.progress();
                    }
                }
            } finally {
                fin.close();
            }
            // don't complete files that aren't done yet. prevents partial files
            // from being written
            fout.close();
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

    public static class CoercerMapper extends AbstractFileCopyMapper {

        RecordStreamFactory factin;
        RecordStreamFactory factout;

        @Override
        protected void copyFile(FileSystem fsSource, Path source, FileSystem fsDest, Path target, Reporter reporter)
                throws IOException {
            RecordInputStream fin = factin.getInputStream(fsSource, source);
            RecordOutputStream fout = factout.getOutputStream(fsDest, target);

            try {
                byte[] record;
                int bytes = 0;
                while ((record = fin.readRawRecord()) != null) {
                    fout.writeRaw(record);
                    bytes += record.length;
                    if (bytes >= 1000000) { // every 1 MB of data report
                                            // progress so we don't time out on
                                            // large files
                        bytes = 0;
                        reporter.progress();
                    }
                }
            } finally {
                fin.close();
            }
            // don't complete files that aren't done yet. prevents partial files
            // from being written
            fout.close();
        }

        @Override
        public void configure(JobConf job) {
            super.configure(job);
            factin = (RecordStreamFactory) Utils.getObject(job, FACTIN_ARG);
            factout = (RecordStreamFactory) Utils.getObject(job, FACTOUT_ARG);
        }
    }
}
