package io.qimia.uhrwerk.common.model;

import com.google.common.base.Objects;
import java.time.LocalDateTime;

abstract class StateModel extends BaseModel {
  LocalDateTime deactivatedTs;
  String description;

  public StateModel(LocalDateTime deactivated) {
    this.deactivatedTs = deactivated;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public LocalDateTime getDeactivatedTs() {
    return deactivatedTs;
  }

  public void setDeactivatedTs(LocalDateTime deactivatedTs) {
    this.deactivatedTs = deactivatedTs;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StateModel)) {
      return false;
    }
    StateModel that = (StateModel) o;
    return Objects.equal(getDeactivatedTs(), that.getDeactivatedTs())
        && Objects.equal(getDescription(), that.getDescription());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getDeactivatedTs(), getDescription());
  }

  @Override
  public String toString() {
    return "StateModel{"
        + "deactivatedTs="
        + deactivatedTs
        + ", description='"
        + description
        + '\''
        + '}';
  }
}
