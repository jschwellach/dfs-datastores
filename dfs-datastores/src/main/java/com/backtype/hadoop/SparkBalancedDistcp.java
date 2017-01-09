package com.backtype.hadoop;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.backtype.hadoop.FileCopyInputFormat.FileCopyArgs;
import com.backtype.support.Utils;

public class SparkBalancedDistcp {
    private static Thread shutdownHook;
    private static RunningJob job = null;

    public static void distcp(String qualifiedSource, String qualifiedDest, int renameMode, PathLister lister)
            throws IOException {
        distcp(qualifiedSource, qualifiedDest, renameMode, lister, "");
    }

    public static void distcp(String qualSource, String qualDest, int renameMode, PathLister lister,
            String extensionOnRename) throws IOException {
        FileCopyArgs args = new FileCopyArgs(qualSource, qualDest, renameMode, lister, extensionOnRename);
        distcp(args);
    }

    public static void distcp(String qualSource, String qualDest, int renameMode, PathLister lister,
            String extensionOnRename, Configuration configuration) throws IOException {
        FileCopyArgs args = new FileCopyArgs(qualSource, qualDest, renameMode, lister, extensionOnRename);
        distcp(args, configuration);
    }

    public static void distcp(FileCopyArgs args) throws IOException {
        distcp(args, new Configuration());
    }

    public static void distcp(FileCopyArgs args, Configuration configuration) throws IOException {
        if (!Utils.hasScheme(args.source) || !Utils.hasScheme(args.dest))
            throw new IllegalArgumentException("source and dest must have schemes " + args.source + " " + args.dest);

        SparkConf conf = new SparkConf();
        // we need to add the following two lines to be able to serialize the
        // java classes of hadoop. This list is comma seperated
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.classesToRegister", "org.apache.hadoop.io.Text")
                .set("spark.kryoserializer.buffer.max value", "2g");

        SparkContext sc = SparkContext
                .getOrCreate(conf.setMaster("local").setAppName("BalancedDistcp: " + args.source + " -> " + args.dest));
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

        BalancedDistcpFunction balancedDistcpFunction = new BalancedDistcpFunction(args, tmpRoot);

        hadoopFile.foreach(balancedDistcpFunction);
        jsc.close();

    }

    public static class BalancedDistcpFunction extends AbstractFileCopyFunction {
        private static final long serialVersionUID = -3878471529570327422L;

        byte[] buffer = new byte[128 * 1024]; // 128 K

        public BalancedDistcpFunction(FileCopyArgs args, String tmpRoot) {
            super(args, tmpRoot);
        }
        @Override
        protected void copyFile(FileSystem fsSource, Path source, FileSystem fsDest, Path target) throws IOException {
            FSDataInputStream fin = fsSource.open(source);
            FSDataOutputStream fout = fsDest.create(target);

            try {
                int amt;
                while ((amt = fin.read(buffer)) >= 0) {
                    fout.write(buffer, 0, amt);
                }
            } finally {
                fin.close();
            }
            // don't complete files that aren't done yet. prevents partial files
            // from being written
            // doesn't really matter though since files are written to tmp file
            // and renamed
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
}
