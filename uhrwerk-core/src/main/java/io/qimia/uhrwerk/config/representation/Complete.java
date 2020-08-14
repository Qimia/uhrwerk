package io.qimia.uhrwerk.config.representation;

import io.qimia.uhrwerk.config.ConfigException;

public class Complete{
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

    public void validate(String path) {
        path += "/";
        if(global==null){
            throw new ConfigException("Missing field:" + path + "global");
        }
        else{
            global.validate(path);
        }
        if(tables==null){
            throw new ConfigException("Missing field:" + path + "tables");
        }
        else{
            for (Table t: tables){
                t.validate(path);
            }
        }
    }
}
