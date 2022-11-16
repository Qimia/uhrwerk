package io.qimia.uhrwerk.config.builders;

public abstract class BaseConnectionBuilder<B extends BaseConnectionBuilder<B>> {
  String name;

  public B name(String name) {
    this.name = name;
    return getThis();
  }

  abstract B getThis();
}
