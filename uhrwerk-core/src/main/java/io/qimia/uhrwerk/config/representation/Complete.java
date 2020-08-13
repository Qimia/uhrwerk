package io.qimia.uhrwerk.config.representation;

import java.util.ArrayList;

public class Complete extends Representation{
    private Uhrwerk uhrwerk;
    private Config config;
    private Table[] tables;

    public Complete() {
    }

    public Uhrwerk getUhrwerk() {
        return uhrwerk;
    }

    public void setUhrwerk(Uhrwerk uhrwerk) {
        this.uhrwerk = uhrwerk;
    }

    public Config getConfig() {
        return config;
    }

    public void setConfig(Config config) {
        this.config = config;
    }

    public Table[] getTables() {
        return tables;
    }

    public void setTables(Table[] tables) {
        this.tables = tables;
    }

    @Override
    public ValidationResult validate() {
        Boolean valid = true;
        String fieldName = "";
        ValidationResult uhrwerkValidation = uhrwerk.validate();
        ValidationResult configValidation = config.validate();
        ArrayList<ValidationResult> tableResults = new ArrayList<ValidationResult>();
        for (Table t: tables){
            ValidationResult v = t.validate();
            if (!v.valid){
                tableResults.add(v);
            }
        }
        if(!uhrwerkValidation.valid){
            valid = false;
            fieldName = "uhrwerk>" + uhrwerkValidation.fieldName + ";";
        }
        if(!configValidation.valid){
            valid = false;
            fieldName = fieldName + "config>" + configValidation.fieldName + ";";
        }
        for(ValidationResult v: tableResults){
            valid = false;
            fieldName = fieldName + "table>" + v.fieldName + ";";
         }
        ValidationResult v = new ValidationResult();
        v.valid=valid;
        v.fieldName=fieldName;
        return v;
    }
}
