package io.qimia.uhrwerk.config.representation;

public class Global extends Representation{
    private Uhrwerk uhrwerk;
    private Config config;

    public Global() {
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

    @Override
    public ValidationResult validate(){
        Boolean valid = true;
        String fieldName = "";
        if (uhrwerk != null){
            ValidationResult uhrwerkValidation = uhrwerk.validate();
            if(!uhrwerkValidation.valid){
                valid = false;
                fieldName = "uhrwerk>" + uhrwerkValidation.fieldName + ";";
            }
        }
        else{
            valid = false;
            fieldName = "uhrwerk";
        }
        if (config != null){
            ValidationResult configValidation = config.validate();
            if(!configValidation.valid){
                valid = false;
                fieldName = fieldName + "config>" + configValidation.fieldName + ";";
            }
        }
        else{
            valid = false;
            fieldName = "config";
        }
        ValidationResult v = new ValidationResult();
        v.valid = valid;
        v.fieldName = fieldName;
        return v;
    }
}
