package io.qimia.uhrwerk.config;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

public class S3InputStream {


  public static InputStream getS3InputStream(String fileLoc) {
    return getS3InputStream(fileLoc, false);
  }


  /**
   * Create an InputStream to read the config directly from S3
   *
   * @param fileLoc String with S3 path
   * @return A file stream which yamlreader can use
   */
  public static InputStream getS3InputStream(String fileLoc, boolean instanceProfile) {
    try {
      URI uri = new URI(fileLoc);
      String scheme = uri.getScheme();
      if (scheme != null && scheme.equals("s3")) {
        String bucket = uri.getHost();
        String path = uri.getPath().replaceFirst("/", "");
        // default client uses com.amazonaws.auth.DefaultAWSCredentialsProviderChain for auth
        AmazonS3 client;
        if (!instanceProfile) {
          client = AmazonS3ClientBuilder.standard().withRegion(Regions.EU_WEST_1).build();
        } else {
          client = AmazonS3ClientBuilder.standard()
              .withRegion(Regions.EU_WEST_1)
              .withCredentials(new InstanceProfileCredentialsProvider(true))
              .build();
        }
        GetObjectRequest request = new GetObjectRequest(bucket, path);
        S3Object object = client.getObject(request);
        return object.getObjectContent();
      } else {
        throw new RuntimeException("Uri scheme is not S3 for " + fileLoc);
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException("Could not find s3 uri scheme in " + fileLoc);
    }
  }
}
