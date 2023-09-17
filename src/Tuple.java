import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;

public class Tuple implements Serializable {
    Hashtable<String,Object> record ;
    String primaryKey;
    String tableName;




    public Tuple(Hashtable<String,Object> record,String tableName,String primaryKey)
    {
        this.record=record;
        this.tableName=tableName;
        this.primaryKey=primaryKey;
    }



    public void insertRecord()
    {
        //insert into the record hashtable here maybe will be changed into vector
        //check the input has all columns , no values are missing



    }

    public Object getTuplePrimaryKeyValue(){return record.get(primaryKey);}
    public Integer getTuplePrimaryKeyValueInteger()
    {
        return (Integer) record.get(primaryKey);
    }
    public String getTuplePrimaryKeyValueString()
    {
        return (String) record.get(primaryKey);
    }
    public Date getTuplePrimaryKeyValueDate()
    {
        return (Date) record.get(primaryKey);
    }
    public Double getTuplePrimaryKeyValueDouble()
    {
        return (Double) record.get(primaryKey);
    }





    //new code !!
    public Hashtable<String, Object> getRecord()
    {
        return record;
    }



    public boolean equalAllValues(Tuple tuple)
    {
        // Get an Enumeration of the keys in the Hashtable
        Enumeration<String> keys = tuple.record.keys();
        Hashtable<String , Object> recordToBeCompared=tuple.record;

        // Iterate over the keys
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            Object value1 = recordToBeCompared.get(key);

            Object value2=record.get(key);

            //if one value does not match then return false
            //is checking objects enough with .equals()??
            if(!value1.equals(value2) && !(value1 instanceof DBAppNull) )
                return false;

        }

        //all values matched
        return true;
    }

    public boolean equalAllValues(Hashtable<String,Object> recordToBeCompared)
    {
        // Get an Enumeration of the keys in the Hashtable
        Enumeration<String> keys = recordToBeCompared.keys();

        // Iterate over the keys
        while (keys.hasMoreElements()) {
            String key = keys.nextElement();
            Object value1 = recordToBeCompared.get(key);

            Object value2=record.get(key);

            //if one value does not match then return false
            //is checking objects enough with .equals()??
            if(!value1.equals(value2) && !(value1 instanceof DBAppNull))
                return false;


        }

        //all values matched
        return true;
    }


    public Tuple updateTuple(Object primaryKeyValue,Hashtable<String,Object> updatedRecord ) throws DBAppException {

        if(!primaryKeyValue.equals(record.get(primaryKey)))//primary keys do not match
            throw new DBAppException("THE CLUSTERING KEY DOES NOT EXIST!!");

        Enumeration<String> keys = updatedRecord.keys();

        //must create a new reference

        Tuple oldTuple=new Tuple((Hashtable<String, Object>) this.getRecord().clone(),this.tableName,this.primaryKey);


        // Iterate over the keys
        while (keys.hasMoreElements()) {

            String key = keys.nextElement();
            Object newValue = updatedRecord.get(key);

//            if(!record.containsKey(key))//invalid key
//                throw new DBAppException();

           //update the tuple
            //record.remove(key);
            record.put(key,newValue);



        }
        return oldTuple;


    }






    public void printTuple()
    {
        String fileName="src"+File.separator+"resource"+ File.separator+"metadata.csv";

        BufferedReader reader =null;
        String line="";

        try{

            reader=new BufferedReader(new FileReader(fileName));
            line=reader.readLine();//first line is irrelevant

            System.out.print(record.get(primaryKey)+" , ");
            while((line=reader.readLine())!=null)
            {
                String []row=line.split(",");
                String tableName=row[0];
                String columnName=row[1];

                if(!columnName.equals(primaryKey) && tableName.equals(this.tableName))
                    System.out.print(record.get(columnName)+" , ");

            }
            System.out.println();
            //printed the column values


        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }


    public boolean allXor(ArrayList<String> columnNames , ArrayList<Object> columnValues, ArrayList<String>operators) throws DBAppException {
        boolean ans =false;
        for(int i=0;i<columnNames.size();i++)
        {
            String columnName=columnNames.get(i);
            Object value=record.get(columnName);
            ans  =ans ^ Compare.compareToWithOperator(value,columnValues.get(i),operators.get(i));

        }
        return ans;

    }


    public boolean allAnd(ArrayList<String> columnNames , ArrayList<Object> columnValues, ArrayList<String>operators) throws DBAppException {
        for(int i=0;i<columnNames.size();i++)
        {
            String columnName=columnNames.get(i);
            Object value=record.get(columnName);
            if(!Compare.compareToWithOperator(value,columnValues.get(i),operators.get(i))) //any one does not satisfy return false
                return false;

        }
        return true;

    }

    public boolean allOr(ArrayList<String> columnNames , ArrayList<Object> columnValues,ArrayList<String>operators) throws DBAppException {
        for(int i=0;i<columnNames.size();i++)
        {
            String columnName=columnNames.get(i);
            Object value=record.get(columnName);
            if(Compare.compareToWithOperator(value,columnValues.get(i),operators.get(i))) //any one satisfy return true
                return true;

        }
        return false;
    }






}
