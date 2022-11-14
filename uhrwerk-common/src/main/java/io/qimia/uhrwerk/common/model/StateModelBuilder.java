package io.qimia.uhrwerk.common.model;

import java.time.LocalDateTime;
import net.openhft.hashing.LongHashFunction;

public abstract class StateModelBuilder<B extends StateModelBuilder<B>> {
  LocalDateTime deactivatedTs;

  public B deactivatedTs(LocalDateTime deactivatedTs) {
    this.deactivatedTs = deactivatedTs;
    return getThis();
  }

  abstract B getThis();
}
