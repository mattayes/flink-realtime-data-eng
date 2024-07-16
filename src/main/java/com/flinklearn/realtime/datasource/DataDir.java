package com.flinklearn.realtime.datasource;

import org.apache.commons.io.FileUtils;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;

public class DataDir {
    public static void clean(String dir) throws Exception {
        //Clean out existing files in the directory
        File f = new File(dir);
        try {
            FileUtils.cleanDirectory(f);
        } catch (IllegalArgumentException e) {
            Files.createDirectory(
                    java.nio.file.Path.of(dir),
                    PosixFilePermissions.asFileAttribute(
                            PosixFilePermissions.fromString("rw-rw-rw-")
                    )
            );
        }
    }
}
