package io.qimia.uhrwerk.config.representation;

import java.util.ArrayList;

public class Complete extends Representation{
    private Global global;
    private Table[] tables;

    public Complete() {
    }

    public Table[] getTables() {
        return tables;
    }

    public void setTables(Table[] tables) {
        this.tables = tables;
    }

    public Global getGlobal() { return global; }

    public void setGlobal(Global global) { this.global = global; }

    public boolean tablesSet() {
        return tables != null;
    }

    @Override
    public ValidationResult validate() {
        Boolean valid = true;
        String fieldName = "";
        if (global != null){
            ValidationResult globalValidation = global.validate();
            if(!globalValidation.valid){
                valid = false;
                fieldName = "global>" + globalValidation.fieldName + ";";
            }
        }
        else{
            valid = false;
            fieldName = "global";
        }
        if (tables.length>0) {
            ArrayList<ValidationResult> tableResults = new ArrayList<ValidationResult>();
            for (Table t : tables) {
                ValidationResult v = t.validate();
                if (!v.valid) {
                    tableResults.add(v);
                }
            }
            for (ValidationResult v : tableResults) {
                valid = false;
                fieldName = fieldName + "table>" + v.fieldName + ";";
            }
        }
        else{
            valid = false;
            fieldName = "tables";
        }
        ValidationResult v = new ValidationResult();
        v.valid=valid;
        v.fieldName=fieldName;
        return v;
    }
}
