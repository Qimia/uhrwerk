package io.qimia.uhrwerk.config;

import static com.google.common.truth.Truth.assertThat;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;

class AWSSecretProviderTest {
  @Test
  void getRegion() {
    Region region = Region.of("eu-west-1");
    assertThat(region).isNotNull();
    boolean contains = Region.regions().contains(region);
    assertThat(contains).isTrue();
    System.out.println(region);
  }

  @Test
  void wrongRegion() {
    Region region = Region.of("not-existing-region");
    boolean contains = Region.regions().contains(region);
    assertThat(contains).isFalse();
    System.out.println(region);
  }

  @Test
  @Disabled
  void secretValue() {
    AWSSecretProvider provider = new AWSSecretProvider("eu-west-1");
    String value = provider.secretValue("/prod/qhr/ymlSecrets");
    assertThat(value).isNotNull();
  }
}
