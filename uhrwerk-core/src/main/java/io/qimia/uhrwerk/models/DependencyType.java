package io.qimia.uhrwerk.models;

public enum DependencyType {
    ONEONONE, AGGREGATE, WINDOW;

    public static DependencyType getDependencyType(String type) {
        final DependencyType resType;
        switch (type.toLowerCase()) {
            case "oneonone":
                resType = ONEONONE;
                break;
            case "agg":
            case "aggregate":
                resType = AGGREGATE;
                break;
            case "window":
                resType = WINDOW;
                break;
            default:
                System.err.println("Bad dependency type given: " + type);
                System.err.println("using one on one instead");
                resType = ONEONONE;
        }
        return resType;
    }
}
