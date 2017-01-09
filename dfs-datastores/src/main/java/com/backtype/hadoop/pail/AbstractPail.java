package com.backtype.hadoop.pail;

import com.backtype.hadoop.formats.RecordInputStream;
import com.backtype.hadoop.formats.RecordOutputStream;
import com.backtype.support.Utils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public abstract class AbstractPail {
    public static Logger LOG = LoggerFactory.getLogger(PailOutputStream.class);
    public static final String EXTENSION = ".pailfile";
    public static final String META_EXTENSION = ".metafile";
    public static final String META_TEMP_EXTENSION = ".metafiletmp";
    private static final String TEMP_EXTENSION = ".pailfiletmp";

    private class PailOutputStream implements RecordOutputStream {

        private Path finalFile;
        private RecordOutputStream delegate;

        public PailOutputStream(String userfilename, boolean overwrite) throws IOException {

            finalFile = new Path(_instance_root, userfilename + EXTENSION);
            if(finalFile.getName().equals(EXTENSION)) throw new IllegalArgumentException("Cannot create empty user file name");

            if(overwrite && exists(finalFile)) {
                delete(finalFile, false);
            }

            if(exists(finalFile)) {
                throw new IOException("File already exists " + finalFile.toString());
            }

            LOG.info(finalFile.toString());
            if (!(Utils.isS3Scheme(finalFile.toString()))) {
                mkdirs(finalFile.getParent());
            }
            delegate = createOutputStream(finalFile);

        }

        public void writeRaw(byte[] record) throws IOException {
            writeRaw(record, 0, record.length);
        }

        public void close() throws IOException {
            delegate.close();
        }

        public void writeRaw(byte[] record, int start, int length) throws IOException {
            delegate.writeRaw(record, start, length);
        }
    }

    private String _instance_root;

    public AbstractPail(String path) throws IOException {
        _instance_root = path;
    }

    public boolean exists(String userfilename) throws IOException {
        return exists(toStoredPath(userfilename));
    }

    public RecordOutputStream openWrite(String userfilename) throws IOException {
        return openWrite(userfilename, false);
    }

    public RecordOutputStream openWrite(String userfilename, boolean overwrite) throws IOException {
        return new PailOutputStream(userfilename, overwrite);
    }

    public RecordInputStream openRead(String userfilename) throws IOException {
        return createInputStream(toStoredPath(userfilename));
    }

    public void deleteMetadata(String metafilename) throws IOException {
        Path metaPath = toStoredMetadataPath(metafilename);
        delete(metaPath, false);
    }

    public void mkAttr(String attr) throws IOException {
        if (!Utils.isS3Scheme(_instance_root)) {
            mkdirs(new Path(_instance_root + "/" + attr));
        }
    }

    public void writeMetadata(String metafilename, String metadata) throws IOException {
        Path metaPath = toStoredMetadataPath(metafilename);
        Path metaTmpPath = toStoredMetadataTmpPath(metafilename);
        if (!Utils.isS3Scheme(metaTmpPath.toString())) {
            mkdirs(metaTmpPath.getParent());
        }
        delete(metaPath, false);
        delete(metaTmpPath, false);
        RecordOutputStream os = createOutputStream(metaTmpPath);
        os.writeRaw(("M" + metadata).getBytes("UTF-8")); //ensure that it's not an empty record
        os.close();
        rename(metaTmpPath, metaPath);
    }

    public String getMetadata(String metafilename) throws IOException {
        Path metaPath = toStoredMetadataPath(metafilename);
        if(exists(metaPath)) {
            RecordInputStream is = createInputStream(metaPath);
            String metaStr = new String(is.readRawRecord(), "UTF-8");
            is.close();
            return metaStr.substring(1);
        } else {
            return null;
        }
    }

    protected abstract RecordInputStream createInputStream(Path path) throws IOException;
    protected abstract RecordOutputStream createOutputStream(Path path) throws IOException;
    protected abstract boolean delete(Path path, boolean recursive) throws IOException;
    protected abstract boolean exists(Path path) throws IOException;
    protected abstract boolean rename(Path source, Path dest) throws IOException;
    protected abstract boolean mkdirs(Path path) throws IOException;
    protected abstract FileStatus[] listStatus(Path path) throws IOException;

    public List<String> getUserFileNames() throws IOException {
        List<String> ret = new ArrayList<String>();
        getFilesHelper(new Path(_instance_root), "", EXTENSION, true, ret);
        return ret;
    }

    public Path toStoredPath(String userfilename) {
        return new Path(_instance_root, userfilename+EXTENSION);
    }

    public Path toStoredMetadataPath(String metadatafilename) {
        return new Path(_instance_root, metadatafilename+META_EXTENSION);
    }

    public Path toStoredMetadataTmpPath(String metadatafilename) {
        return new Path(_instance_root, metadatafilename+META_TEMP_EXTENSION);
    }


    public void delete(String userfilename) throws IOException {
        delete(toStoredPath(userfilename), false);
    }

    public List<Path> getStoredFiles() throws IOException {
        List<String> userfiles = getUserFileNames();
        List<Path> ret = new ArrayList<Path>();
        for(String u: userfiles) {
            ret.add(toStoredPath(u));
        }
        return ret;
    }


    public List<String> getMetadataFileNames() throws IOException {
        List<String> ret = new ArrayList<String>();
        getFilesHelper(new Path(_instance_root), "", META_EXTENSION, true, ret);
        return ret;
    }

    public List<Path> getStoredMetadataFiles() throws IOException {
        List<String> userfiles = getMetadataFileNames();
        List<Path> ret = new ArrayList<Path>();
        for(String u: userfiles) {
            ret.add(toStoredMetadataPath(u));
        }
        return ret;
    }

    public List<Path> getStoredFilesAndMetadata() throws IOException {
        List<String> relFiles = new ArrayList<String>();
        List<String> extensions = new ArrayList<String>();
        extensions.add(META_EXTENSION);
        extensions.add(EXTENSION);
        getFilesHelper(new Path(_instance_root), "", extensions, false, relFiles);
        List<Path> ret = new ArrayList<Path>();
        for(String rel: relFiles) {
            ret.add(new Path(_instance_root, rel));
        }
        return ret;
    }


    public List<Path> getStoredUnfinishedFiles() throws IOException {
        List<String> userfiles = new ArrayList<String>();
        getFilesHelper(new Path(_instance_root), "", TEMP_EXTENSION, true, userfiles);
        List<Path> ret = new ArrayList<Path>();
        for(String u: userfiles) {
            ret.add(new Path(_instance_root, u+TEMP_EXTENSION));
        }
        return ret;
    }

    protected List<String> readDir(String subdir, boolean dir) throws IOException {
        Path absDir;
        if(subdir.length()==0) {
            absDir = new Path(_instance_root);
        } else {
            absDir = new Path(_instance_root, subdir);
        }

        List<String> ret = new ArrayList<String>();
        FileStatus[] contents = listStatus(absDir);
        for(FileStatus fs: contents) {
            String name = fs.getPath().getName();
            if((fs.isDir() && dir || !fs.isDir() && !dir) && !name.contains("_")) {
                ret.add(name);
            }
        }
        return ret;
    }

    public List<String> getAttrsAtDir(String subdir) throws IOException {
        return readDir(subdir, true);
    }

    public List<String> getMetadataFileNames(String subdir) throws IOException {
        List<String> files = readDir(subdir, false);
        List<String> ret = new ArrayList<String>();
        for(String f: files) {
            if(f.endsWith(META_EXTENSION)) {
                ret.add(Utils.stripExtension(f, META_EXTENSION));
            }
        }
        return ret;
    }

    public String getInstanceRoot() {
        return _instance_root;
    }

    private String relify(String root, String name) {
        if(root.length()==0) return name;
        else return new Path(root, name).toString();
    }

    private void getFilesHelper(Path abs, String rel, String extension, boolean stripExtension, List<String> files) throws IOException {
        List<String> extensions = new ArrayList<String>();
        extensions.add(extension);
        getFilesHelper(abs, rel, extensions, stripExtension, files);
    }

    private void getFilesHelper(Path abs, String rel, List<String> extensions, boolean stripExtension, List<String> files) throws IOException {
        FileStatus[] contents = listStatus(abs);
        for(FileStatus stat: contents) {
            Path p = stat.getPath();
            if(stat.isDir()) {
                getFilesHelper(p, relify(rel, stat.getPath().getName()), extensions, stripExtension, files);
            } else {
                String filename = relify(rel, stat.getPath().getName());
                for(String extension: extensions) {
                    if(filename.endsWith(extension) && stat.getLen()>0) {
                        String toAdd;
                        if(stripExtension) {
                            toAdd = Utils.stripExtension(filename, extension);
                        } else {
                            toAdd = filename;
                        }
                        files.add(toAdd);
                        break;
                    }
                }
            }
        }
    }
}
