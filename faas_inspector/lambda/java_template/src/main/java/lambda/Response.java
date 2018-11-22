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

    // Return tablename
    private String tablename;
    public String getTablename()
    {
        return tablename;
    }
    public void setTablename(String tablename)
    {
        this.tablename = tablename;
    }
    
    public String dbname;
    public String getDbname()
    {
        return this.dbname;
    }
    public void setDbname(String dbname)
    {
        this.dbname = dbname;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("dbName=");
        sb.append(this.getDbname());
        sb.append(" | ");
        sb.append("tableName=");
        sb.append(this.getTablename());
        return sb + super.toString();
    }

}
