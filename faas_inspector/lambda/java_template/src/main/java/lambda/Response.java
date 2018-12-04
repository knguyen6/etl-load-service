/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

import faasinspector.fiResponse;
import java.util.List;

/**
 *
 * @author wlloyd
 */
public class Response extends fiResponse {


    //
    // User Defined Attributes
    //
    //
    // ADD getters and setters for custom attributes here.
    //
    private boolean success;
    private String bucketname;
    private String tablename;
    public String dbname;



    private String error;


    public String getBucketname() {
        return bucketname;
    }

    public void setBucketname(String bucketname) {
        this.bucketname = bucketname;
    }
    public String getTablename()
    {
        return tablename;
    }
    public void setTablename(String tablename)
    {
        this.tablename = tablename;
    }
    public String getDbname()
    {
        return this.dbname;
    }
    public void setDbname(String dbname)
    {
        this.dbname = dbname;
    }

    public boolean getSuccess() {
        return success;
    }

    public void setSuccess(boolean message) {
        this.success = message;
    }
    @Override
    public String getError() {
        return error;
    }

    @Override
    public void setError(String error) {
        this.error = error;
    }
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("bucketname=");
        sb.append(this.getBucketname());
        sb.append(" | ");
        sb.append("dbname=");
        sb.append(this.getDbname());
        sb.append(" | ");
        sb.append("tablename=");
        sb.append(this.getTablename());

        return sb + super.toString();
    }

}
