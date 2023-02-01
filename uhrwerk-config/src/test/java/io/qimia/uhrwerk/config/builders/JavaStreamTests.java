package io.qimia.uhrwerk.config.builders;


import static com.google.common.truth.Truth.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

public class JavaStreamTests {

  @Test
  public void emptyList() {
    List<String> list = new ArrayList<>();
    list.add("someThing");
    list.add("foo1");
    list.add("Foo");
    boolean foo = list.stream().anyMatch(arg -> arg.equalsIgnoreCase("foo"));
    assertThat(foo).isTrue();
    List<String> empty = new ArrayList<>();
    boolean foo2 = empty.stream().anyMatch(arg -> arg.equalsIgnoreCase("foo"));
    assertThat(foo2).isFalse();

    empty.addAll(list);
    assertThat(empty).containsExactly("someThing", "foo1", "Foo");

    var emptyEmpty = new ArrayList<>();
    emptyEmpty.addAll(new ArrayList<>());
    assertThat(emptyEmpty).isEmpty();

  }

}
