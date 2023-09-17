import java.io.Serializable;

public class Refrence implements Serializable {




    int pageId;
    int recordId;

    Object key;
    //Tuple tuple;

    public Refrence(int pageid,int recordId,Object key)
    {
        this.pageId=pageid;
        this.recordId=recordId;
        this.key=key;
        //this.tuple=tuple;
    }


    public int getPageId() {
        return pageId;
    }

    public int getRecordId() {
        return recordId;
    }

    public Object getKey() {
        return key;
    }


    public String toString()
    {
        return "pageId "+pageId+" recordId "+recordId+" KEY "+key;//+" Tuple reference "+ (tuple==null ? ("NULL") : (tuple.toString()));
    }

}
