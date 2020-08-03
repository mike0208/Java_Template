package com.example.activemq;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public class FileAsByteArrayManager {
    public FileAsByteArrayManager() {
    }

    public byte[] readfileAsBytes(File file) throws IOException {
        RandomAccessFile accessFile = new RandomAccessFile(file, "r");
        Throwable var3 = null;

        byte[] var5;
        try {
            byte[] bytes = new byte[(int)accessFile.length()];
            accessFile.readFully(bytes);
            var5 = bytes;
        } catch (Throwable var14) {
            var3 = var14;
            throw var14;
        } finally {
            if (accessFile != null) {
                if (var3 != null) {
                    try {
                        accessFile.close();
                    } catch (Throwable var13) {
                        var3.addSuppressed(var13);
                    }
                } else {
                    accessFile.close();
                }
            }

        }

        return var5;
    }

    public void writeFile(byte[] bytes, String fileName) throws IOException {
        File file = new File(fileName);
        RandomAccessFile accessFile = new RandomAccessFile(file, "rw");
        Throwable var5 = null;

        try {
            accessFile.write(bytes);
        } catch (Throwable var14) {
            var5 = var14;
            throw var14;
        } finally {
            if (accessFile != null) {
                if (var5 != null) {
                    try {
                        accessFile.close();
                    } catch (Throwable var13) {
                        var5.addSuppressed(var13);
                    }
                } else {
                    accessFile.close();
                }
            }

        }

    }
}
