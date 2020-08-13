package io.qimia.uhrwerk.config.representation;

import java.util.ArrayList;

public class Config extends Representation{
    private Connection[] connections;

    public Config(){}

    public Connection[] getConnections() {
        return connections;
    }

    public void setConnections(Connection[] connections) {
        this.connections = connections;
    }

    @Override
    public ValidationResult validate(){
        Boolean valid = true;
        String fieldName = "";
        ArrayList<ValidationResult> connectionResults = new ArrayList<ValidationResult>();
        for (Connection c: connections){
            ValidationResult v = c.validate();
            if (!v.valid){
                connectionResults.add(v);
            }
        }
        for (ValidationResult v: connectionResults){
            valid = false;
            fieldName = "table>" + v.fieldName + ";";
        }
        ValidationResult v = new ValidationResult();
        v.valid=valid;
        v.fieldName=fieldName;
        return v;
    }
}
