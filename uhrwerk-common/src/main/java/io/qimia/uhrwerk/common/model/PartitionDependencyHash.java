package io.qimia.uhrwerk.common.model;

import net.openhft.hashing.LongHashFunction;

public class PartitionDependencyHash {

    /**
     * Generate a unique Id for a partition combination (assumes keys have been set)
     * @param childId the partition that has dependencies
     * @param parentId the partition the child is depending on
     * @return long value used as id / key
     */
    public static Long generateId(Long childId, Long parentId) {
        StringBuilder res = new StringBuilder().append(childId).append(parentId);
        return LongHashFunction.xx().hashChars(res);
    }
}
