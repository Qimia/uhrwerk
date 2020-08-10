package io.qimia.uhrwerk.config.dao.data;

import io.qimia.uhrwerk.backend.model.data.Target;

public class TargetDAO {
    public static Target convertTargetToBackend(io.qimia.uhrwerk.config.model.Target target) {
        Target backendTarget = new Target();
        backendTarget.setPath(target.getPath());

        return backendTarget;
    }
}
