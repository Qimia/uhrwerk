package io.qimia.uhrwerk.config;

import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.secretsmanager.model.SecretsManagerException;

public class AWSSecretProvider {

  private final Region region;

  private final boolean awsInstanceProfile;
  private final SecretsManagerClient secretsClient;

  public AWSSecretProvider(String region) {
    this(region, false);
  }

  public AWSSecretProvider(String region, boolean awsInstanceProfile) {
    this.awsInstanceProfile = awsInstanceProfile;
    this.region = Region.of(region);
    if (!Region.regions().contains(this.region)) {
      throw new IllegalArgumentException(
          String.format("The given region=%s doesn't exist.", region));
    }
    if (this.awsInstanceProfile) {
      this.secretsClient =
          SecretsManagerClient.builder()
              .region(this.region)
              .credentialsProvider(InstanceProfileCredentialsProvider.create())
              .build();
    } else {
      this.secretsClient = SecretsManagerClient.builder()
          .region(this.region)
          .build();
    }
  }

  public String secretValue(String secretName) {
    try {
      GetSecretValueRequest valueRequest =
          GetSecretValueRequest.builder().secretId(secretName).build();

      GetSecretValueResponse valueResponse = secretsClient.getSecretValue(valueRequest);
      String secret = valueResponse.secretString();
      return secret;
    } catch (SecretsManagerException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
    }
    return null;
  }
}
