package com.backtype.hadoop;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.backtype.hadoop.FileCopyInputFormat.FileCopyArgs;
import com.backtype.support.Utils;

import scala.Tuple2;

public abstract class AbstractFileCopyFunction implements VoidFunction<Tuple2<Text, Text>> {
    private static final long serialVersionUID = 6660048991470576767L;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractFileCopyFunction.class);
    private transient FileSystem fsSource;
    private transient FileSystem fsDest;
    private String tmpRoot;
    private FileCopyArgs args;

    public AbstractFileCopyFunction(FileCopyArgs args, String tmpRoot) {
        this.args = args;
        this.tmpRoot = tmpRoot;
    }

    private void setStatus(String msg) {
        LOG.info(msg);
    }

    @Override
    public void call(Tuple2<Text, Text> t) throws Exception {
        // FIXME: this is not using the configuration for getting the
        // filesystem. It should use
        // Utils.getFS(args,fsUri,conf); but HadoopConfiguration is not
        // serializable
        fsSource = Utils.getFS(args.source);
        fsDest = Utils.getFS(args.dest);

        Path sourceFile = new Path(t._1.toString());
        Path finalFile = new Path(t._2.toString());
        Path tmpFile = new Path(tmpRoot, UUID.randomUUID().toString());

        if (fsDest.exists(finalFile)) {
            FileChecksum fc1 = fsSource.getFileChecksum(sourceFile);
            FileChecksum fc2 = fsDest.getFileChecksum(finalFile);
            if (fc1 != null && fc2 != null && !fc1.equals(fc2)
                    || fsSource.getContentSummary(sourceFile).getLength() != fsDest.getContentSummary(finalFile)
                            .getLength()
                    || ((fc1 == null || fc2 == null)
                            && !Utils.firstNBytesSame(fsSource, sourceFile, fsDest, finalFile, 1024 * 1024))) {
                throw new IOException("Target file already exists and is different! " + finalFile.toString());
            } else {
                return;
            }
        }

        if (fsDest.getUri().getScheme().equalsIgnoreCase("hdfs")) {
            setStatus("Copying " + sourceFile.toString() + " to " + tmpFile.toString());
            fsDest.mkdirs(tmpFile.getParent());
            copyFile(fsSource, sourceFile, fsDest, tmpFile);

            setStatus("Renaming " + tmpFile.toString() + " to " + finalFile.toString());

            fsDest.mkdirs(finalFile.getParent());
            if (!fsDest.rename(tmpFile, finalFile))
                throw new IOException("could not rename " + tmpFile.toString() + " to " + finalFile.toString());
        } else {
            setStatus("Copying " + sourceFile.toString() + " to " + finalFile.toString());
            fsDest.mkdirs(finalFile.getParent());
            copyFile(fsSource, sourceFile, fsDest, finalFile);
        }

    }

    protected abstract void copyFile(FileSystem fsSource, Path source, FileSystem fsDest, Path target)
            throws IOException;

}
