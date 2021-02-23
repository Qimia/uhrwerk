package io.qimia.uhrwerk.config;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BlobInputStream {
  private static final Pattern pattern =
      Pattern.compile(
          "https:\\/\\/(\\w+)\\.blob\\.core\\.windows\\.net\\/(\\w+)\\/([\\w\\-\\/.]+);(.+)");

  public static InputStream getBlobInputStream(String location) {
    Matcher match = pattern.matcher(location);

    if (!match.matches()) {
      System.err.println("Could not match azure connection string: " + location);
      throw new RuntimeException("Bad Azure location");
    }
    String account = match.group(1);
    String container = match.group(2);
    String path = match.group(3);
    String connInfo = match.group(4);

    CloudStorageAccount storageAccount = null;
    try {
      storageAccount = CloudStorageAccount.parse(connInfo);
      CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
      CloudBlobContainer blobContainerClient = blobClient.getContainerReference(container);
      CloudBlob configBlob = blobContainerClient.getBlobReferenceFromServer(path);
      return configBlob.openInputStream();
    } catch (URISyntaxException e) {
      e.printStackTrace();
      throw new RuntimeException("URI Syntax of storage account false");
    } catch (InvalidKeyException e) {
      e.printStackTrace();
      throw new RuntimeException("False key");
    } catch (StorageException e) {
      e.printStackTrace();
      throw new RuntimeException("StorageException thrown");
    }
  }
}
