package io.qimia.uhrwerk.config.representation;

public class Uhrwerk extends Representation{
    private Metastore metastore;

    public Uhrwerk() {
    }

    public Metastore getMetastore() {
        return metastore;
    }

    public void setMetastore(Metastore metastore) {
        this.metastore = metastore;
    }

    @Override
    public ValidationResult validate(){
        Boolean valid = true;
        String fieldName = "";
        if(metastore != null){
            ValidationResult metastoreValidation = metastore.validate();
            if (!metastoreValidation.valid){
                valid = true;
                fieldName = "metastore>" + metastoreValidation.fieldName;
            }
        }
        else{
            valid = false;
            fieldName = "metastore";
        }
        ValidationResult v = new ValidationResult();
        v.valid = valid;
        v.fieldName = fieldName;
        return v;
    }
}
