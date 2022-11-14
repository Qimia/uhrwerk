package io.qimia.uhrwerk.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class MapperUtils {
  static String readQueryOrFileLines(String queryOrFile) {
    if (queryOrFile.endsWith(".sql")) {
      try {
        InputStream inputStream = MapperUtils.getInputStream(queryOrFile);
        return new BufferedReader(new InputStreamReader(inputStream))
            .lines()
            .collect(Collectors.joining(" "));
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Something went wrong with reading the sql query file: " + queryOrFile, e);
      }
    } else {
      return queryOrFile;
    }
  }
  static InputStream getInputStream(String file) {
    InputStream stream;
    if (file.contains("s3:")) {
      return io.qimia.uhrwerk.config.S3InputStream.getS3InputStream(file);
    } else if (file.contains(".blob.core.windows.net")) {
      return io.qimia.uhrwerk.config.BlobInputStream.getBlobInputStream(file);
    }
    stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
    if (stream == null) {
      try {
        File fileO = new java.io.File(file);
        stream = new FileInputStream(fileO);
      } catch (Exception f) {
        throw new IllegalArgumentException(
            "Could not read the file. Please check your file paths.", f);
      }
    }
    return stream;
  }

}
