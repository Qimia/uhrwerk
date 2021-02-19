package io.qimia.uhrwerk.config;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;

import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BlobInputStream {
  private static final Pattern pattern =
      Pattern.compile("https:\\/\\/(\\w+)\\.blob\\.core\\.windows\\.net\\/(\\w+)\\/(.+)");

  // https://uhrwerkazureblobtest.blob.core.windows.net/configs/dag-config.yml
  // https://uhrwerkstoragelaketest.blob.core.windows.net/configs/dag-config.yml

  public static InputStream getBlobInputStream(String location) throws Throwable {
    Matcher match = pattern.matcher(location);

    if (!match.matches()) {
      throw new RuntimeException("Bad Azure location");
    }
    String account = match.group(1);
    String container = match.group(2);
    String path = match.group(3);

    CloudStorageAccount storageAccount = CloudStorageAccount.parse(location);
    CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
  }
}
