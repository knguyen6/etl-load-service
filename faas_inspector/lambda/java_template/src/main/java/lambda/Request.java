/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lambda;

/**
 *
 * @author wlloyd
 */
public class Request {
    String bucketname;
    String filename;


    public String getBucketname()
    {
        return bucketname;
    }
    public void setBucketname(String bucketname)
    {
        this.bucketname = bucketname;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public Request()
    {
        
    }
}
