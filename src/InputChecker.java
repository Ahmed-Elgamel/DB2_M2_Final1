import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class InputChecker {




    public static void checkClusteringKey(Table table , Hashtable<String,Object> record) throws DBAppException {
        CsvReader reader=new CsvReader();


        ColumnMetadata columnMetadata = null;

        if(!record.containsKey(table.clusteringColumnName))
            throw new DBAppException("THE RECORD DOES NOT CONTAIN A CLUSTERING KEY");


        try {
            columnMetadata = CsvReader.getColumnMetadata(table.tableName, table.clusteringColumnName);
        } catch (IOException e) {
            throw new RuntimeException(e);
          }



        Object value =record.get(table.clusteringColumnName);
        String min=columnMetadata.getMin();
        String max=columnMetadata.getMax();
        String type=columnMetadata.getColumnType();

        if(!( value.getClass().getName().equalsIgnoreCase(type) && compareTo(value,min,type)>=0 && compareTo(value,max,type)<=0 ))
            throw new DBAppException("THE RECORD HAS "+columnMetadata.getColumnName()+" = "+ value +" WHICH VIOLATES THE COLUMN CONSTRAINTS!!");





    }

    public static void checkColumn (ColumnMetadata columnMetadata , Object value) throws DBAppException {
        String  min=columnMetadata.getMin();
        String max=columnMetadata.getMax();
        String type=columnMetadata.getColumnType();

        if(!(value.getClass().getName().equalsIgnoreCase(type) && compareTo(value,min,type)>=0 && compareTo(value,max,type)<=0 ))
        {
            throw new DBAppException("THE RECORD HAS "+columnMetadata.getColumnName()+" = "+ value +" WHICH VIOLATES THE COLUMN CONSTRAINTS!!");
        }




    }

    public static void checkAllKeys(Table table , Hashtable<String,Object> record,boolean addNulls) throws DBAppException {
        CsvReader reader=new CsvReader();

        Map<String,ColumnMetadata> eachColumnMetaData= null;
        try {
            eachColumnMetaData = CsvReader.readCsvFile(table.tableName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        for(Map.Entry<String,ColumnMetadata> entry : eachColumnMetaData.entrySet())
        {
            String Key =entry.getKey();
            ColumnMetadata columnMetadata=entry.getValue();

            if(record.containsKey(Key))
            {
                Object value=record.get(Key);
                if(!value.getClass().getName().equalsIgnoreCase(DBAppNull.class.getName()))
                    checkColumn(columnMetadata,value);
            }
            else if (addNulls)
            {
                Object Null=new DBAppNull(null);
                record.put(Key,Null);
            }




        }

        for(String Key: record.keySet())
        {
            if(!eachColumnMetaData.containsKey(Key))
                throw new DBAppException("THE RECORD CONTAINS AN UNKNOWN KEY!!");
        }

//        if(record.size()==0)
//            throw new DBAppException("THE RECORD IS EMPTY!!");




    }


    public static Object checkClusteringKeyEncapsulatedInString(Table table , String strClusteringKeyValue) throws DBAppException {
        CsvReader reader=new CsvReader();

        Map<String,ColumnMetadata> eachColumnMetaData= null;
//        try {
//            eachColumnMetaData = CsvReader.readCsvFile(table.tableName);
//
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }

        //ColumnMetadata clusteringKeyMetaData = eachColumnMetaData.get(table.clusteringColumnName);

        ColumnMetadata clusteringKeyMetaData = null;
        try {
            clusteringKeyMetaData = CsvReader.getColumnMetadata(table.tableName , table.clusteringColumnName);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String type=clusteringKeyMetaData.getColumnType();
        String min=clusteringKeyMetaData.getMin();
        String max=clusteringKeyMetaData.getMax();


        Object value=null;

        if(type.equalsIgnoreCase(String.class.getName()))
        {
            value=strClusteringKeyValue;
        }
        else if (type.equalsIgnoreCase(Integer.class.getName()))
        {
            value=Integer.parseInt(strClusteringKeyValue);
        }
        else if (type.equalsIgnoreCase(Double.class.getName()))
        {
            value=Double.parseDouble(strClusteringKeyValue);
        }
        else if (type.equalsIgnoreCase(Date.class.getName()))
        {

            try {
                value = new SimpleDateFormat("yyyy-MM-dd").parse(strClusteringKeyValue);

            } catch (Exception e) {
                System.out.println("DATE FORMAT ERROR!!");
            }
        }
        else
            throw new DBAppException("ERROR IN CLUSTERING KEY TYPE!!");


        if(!(compareTo(value,min,type)>=0 && compareTo(value,max,type)<=0 ))
            throw new DBAppException("THE RECORD HAS "+clusteringKeyMetaData.getColumnName()+" = "+ value +" WHICH VIOLATES THE COLUMN CONSTRAINTS!!");

        return value;


    }

    public static void validateCsvArray(String [][]arr) throws DBAppException
    {
        boolean clusteringKeyFound=false;
        for(int i=1;i<arr.length;i++)
        {
            for(int j=0;j<arr[i].length;j++)
            {
                if(j==1) //column name
                {
                    if(arr[i][j].equals(""))
                        throw new DBAppException("NAME OF COLUMN IS MISSING FOR TABLE "+arr[i][0]+" !!");
                }
                else if(j==2)//column type
                {
                    if(!(arr[i][j].equalsIgnoreCase(String.class.getName()) || arr[i][j].equalsIgnoreCase(Integer.class.getName()) ||
                            arr[i][j].equalsIgnoreCase(Double.class.getName()) || arr[i][j].equalsIgnoreCase(Date.class.getName())))
                        throw new DBAppException("UNSUPPORTED TYPE IN TABLE "+ arr[i][0] +" !!");

                }
                else if (j==3)
                {
                    if(arr[i][j].equals("true"))
                        clusteringKeyFound=true;
                }
                //NO CHECKING ON INDEX

                else if (j==6)//min
                {
                    if(arr[i][j].equals(""))
                        throw new DBAppException("MIN OF COLUMN "+arr[i][1]+" IS MISSING FOR TABLE "+arr[i][0]+" !!");

                }
                else if (j==7)//max
                {
                    if(arr[i][j].equals(""))
                        throw new DBAppException("MIN OF COLUMN "+arr[i][1]+" IS MISSING FOR TABLE "+arr[i][0]+" !!");
                }

            }
        }
        if(!clusteringKeyFound)
            throw new DBAppException("YOU MUST DEFINE A COLUMN TO BE A CLUSTERING KEY !!");
    }


    public static int compareTo(Object a , String b,String type) throws DBAppException {
        if(type.equalsIgnoreCase(Integer.class.getName()))
        {
            Integer x=(Integer) a;
            Integer y=Integer.parseInt(b);
            return x.compareTo(y);

        }
        else if(type.equalsIgnoreCase(Double.class.getName()))
        {
            Double x=(Double) a;
            Double y=Double.parseDouble(b);
            return x.compareTo(y);


        }
        else if (type.equalsIgnoreCase(String.class.getName()))
        {
            String x=(String) a;
            String y=b;
            return (x.compareTo(y));

        }
        else if (type.equalsIgnoreCase(Date.class.getName()))
        {
            Date x=(Date) a;
            Date y= null;
            try {
                y = new SimpleDateFormat("yyyy-MM-dd").parse(b);

            } catch (Exception e) {
                System.out.println("DATE FORMAT ERROR!!");
            }
            return x.compareTo(y);


        }
        else
        {
            //System.out.println(a + " "+ b);
            throw new DBAppException("ERROR IN CLUSTERING KEY TYPE!!");
        }

    }


    public static void checkIndex(String tableName , String indexName,String column1,String column2,String column3) throws IOException, DBAppException {
        String octreeName ;

        octreeName = CsvReader.columnHasIndex(tableName,column1);
        if(octreeName!=null)
            throw new DBAppException("COLUMN "+column1+" ALREADY IS IN AN INDEX CALLED "+octreeName);

        octreeName = CsvReader.columnHasIndex(tableName,column2);
        if(octreeName!=null)
            throw new DBAppException("COLUMN "+column2+" ALREADY IS IN AN INDEX CALLED "+octreeName);

        octreeName = CsvReader.columnHasIndex(tableName,column3);
        if(octreeName!=null)
            throw new DBAppException("COLUMN "+column3+" ALREADY IS IN AN INDEX CALLED "+octreeName);

        Map<String , ArrayList<String>> allOctrees=CsvReader.getAllOctrees(tableName);
        if(allOctrees.containsKey(indexName))
            throw new DBAppException("INDEX WITH NAME "+indexName+" ALREADY EXISTS!!");

        Map<String,ColumnMetadata> allColumnsMetaData = CsvReader.readCsvFile(tableName);

        if(!allColumnsMetaData.containsKey(column1))
            throw new DBAppException("COLUMN "+column1 +" DOES NOT EXIST IN TABLE "+tableName);

        if(!allColumnsMetaData.containsKey(column2))
            throw new DBAppException("COLUMN "+column2 +" DOES NOT EXIST IN TABLE "+tableName);

        if(!allColumnsMetaData.containsKey(column3))
            throw new DBAppException("COLUMN "+column3 +" DOES NOT EXIST IN TABLE "+tableName);


    }


    public static boolean columnExists(String tableName , String columnName) throws IOException {
        Map<String,ColumnMetadata> allColumnsMetaData = CsvReader.readCsvFile(tableName);

        if(allColumnsMetaData.containsKey(columnName))
        {
            return true;
        }

        return false;

    }



}
