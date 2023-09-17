import java.io.Serializable;

public class DBAppNull implements Serializable {
    Object value;

    public DBAppNull(Object value) throws DBAppException {

        if(value!=null)
            throw new DBAppException();
        else
            this.value=value;

    }

    public String toString()
    {
        return "NULL";
    }
}
