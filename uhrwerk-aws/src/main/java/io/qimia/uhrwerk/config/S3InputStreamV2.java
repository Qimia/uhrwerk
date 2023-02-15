package io.qimia.uhrwerk.config;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;


import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3InputStreamV2 {


  /**
   * Create an InputStream to read the config directly from S3
   *
   * @param fileLoc String with S3 path
   * @return A file stream which yamlreader can use
   */
  public static InputStream getS3InputStreamV2(String fileLoc,
      String regionName,
      boolean useProfile) {
    try {
      Region region = Region.of(regionName);
      assert (Region.regions().contains(region)) :
          "The given region=" + regionName + " doesn't exist.";

      S3Client s3Client;
      if (useProfile) {
        s3Client = S3Client.builder()
            .credentialsProvider(ProfileCredentialsProvider.create())
            .region(region).build();
      } else {
        s3Client = S3Client.builder()
            .region(region).build();
      }

      URI uri = new URI(fileLoc);
      String scheme = uri.getScheme();
      if (scheme != null && scheme.equals("s3")) {
        String bucket = uri.getHost();
        String path = uri.getPath().replaceFirst("/", "");
        GetObjectRequest objectRequest = GetObjectRequest
            .builder()
            .key(path)
            .bucket(bucket)
            .build();

        ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
        return objectBytes.asInputStream();

      } else {
        throw new RuntimeException("Uri scheme is not S3 for " + fileLoc);
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException("Could not find s3 uri scheme in " + fileLoc);
    } catch (S3Exception e) {
      System.err.println(e.awsErrorDetails().errorMessage());
    }
    return null;
  }
}
