import java.io.*;
import java.util.*;


public class Page implements Serializable {


    static Integer maxSize;          //max num of records page can have static because all pages share this attribute
    Integer currentSize;            //current number of records present in page
    Object maxKey;                 //max value of key currently in this page     for binary search
    Object minKey;                //minimum value of currently key in this page for binary search
    Vector<Tuple> allRecords;    //vector of the actual records (just like an array)
    Integer pageId;             //page has an id in order to load it from the disk
    Integer maxIndex;          //maximum index that contains a record probably will be used in shifting





    public static void setMaxSize(Integer n)
   {
       maxSize=n;
   }
    public Integer getMaxSize() {
        return maxSize;
    }

    public Object getMaxKey() {
        return maxKey;
    }

    public Object getMinKey() {
        return minKey;
    }

    public Vector<Tuple> getAllRecords() {
        return allRecords;
    }

    public Integer getPageId() {
        return pageId;
    }

    public Integer getCurrentSize() {
        return currentSize;
    }


    public Page(int pageId)
   {
       this.pageId=pageId;
       currentSize=0;
       allRecords=new Vector<>();
   }


   public void insertFirst(Tuple tuple) //inserts in a newly created page
   {
       allRecords.add(0,tuple);
       currentSize++;
       minKey=tuple.getTuplePrimaryKeyValue();
       maxKey=tuple.getTuplePrimaryKeyValue();
   }





    public Tuple genericInsert (Tuple tuple) throws DBAppException {


        if(currentSize<maxSize)
        {
            Object primaryKeyValue = tuple.getTuplePrimaryKeyValue();
            //i got the value of the primary key to know where to insert the tuple in the page


            //get the position the tuple must be inserted into
            int indexOfTuple=getTupleIndex(primaryKeyValue);



            if(indexOfTuple<allRecords.size() && allRecords.get(indexOfTuple).getTuplePrimaryKeyValue().equals(tuple.getTuplePrimaryKeyValue()))
                throw new DBAppException("THE RECORD CONTAINS KEY "+tuple.primaryKey +" = "+tuple.getTuplePrimaryKeyValue()+" WHICH IS A DUPLICATE");

            allRecords.add(indexOfTuple,tuple); //the page will shift automatically

            if(maxKey==null ||compareTo(primaryKeyValue,maxKey)>0) //update maxKey of page
                maxKey=primaryKeyValue;

            if(minKey==null ||compareTo(primaryKeyValue,minKey)<0) //update minKey of page
                minKey=primaryKeyValue;

            currentSize++;


            minKey=allRecords.get(0).getTuplePrimaryKeyValue();
            maxKey=allRecords.get(allRecords.size()-1).getTuplePrimaryKeyValue();
            return null;
        }
        else  //page is full!
        {
            Object primaryKeyValue = tuple.getTuplePrimaryKeyValue();
            //i got the value of the primary key to know where to insert the tuple in the page

            int indexOfTuple=getTupleIndex(primaryKeyValue);

            if(indexOfTuple<allRecords.size() && allRecords.get(indexOfTuple).getTuplePrimaryKeyValue().equals(tuple.getTuplePrimaryKeyValue()))
                throw new DBAppException("THE RECORD CONTAINS KEY "+tuple.primaryKey +" = "+tuple.getTuplePrimaryKeyValue()+" WHICH IS A DUPLICATE");


            Tuple removedTuple=allRecords.remove(currentSize-1);//remove last record in this page



            if(compareTo(primaryKeyValue,maxKey)<0)//the tuple i want to insert is smaller than the maxKey of this page
            {

                allRecords.add(indexOfTuple,tuple);

                if(compareTo(primaryKeyValue,maxKey)>0 || allRecords.size()==1) //update maxKey of page
                    maxKey=allRecords.get(allRecords.size()-1).getTuplePrimaryKeyValue();

                if(compareTo(primaryKeyValue,minKey)<0 || allRecords.size()==1) //update minKey of page
                    minKey=allRecords.get(0).getTuplePrimaryKeyValue();


                minKey=allRecords.get(0).getTuplePrimaryKeyValue();
                maxKey=allRecords.get(allRecords.size()-1).getTuplePrimaryKeyValue();
                return removedTuple;
            }
            else if(compareTo(primaryKeyValue,maxKey)>0)//the tuple i want to insert is bigger than the maxKey of this page
            {
                allRecords.addElement(removedTuple);


                minKey=allRecords.get(0).getTuplePrimaryKeyValue();
                maxKey=allRecords.get(allRecords.size()-1).getTuplePrimaryKeyValue();
                return tuple;  //insert this tuple in the next record
            }
            else
                throw  new DBAppException("I THINK THE KEY "+tuple.getTuplePrimaryKeyValue() +" MUST BE A DUPLICATE");

        }


    }






    public int getTupleIndex(Object primaryKeyValue) throws DBAppException {

        return genericTupleSearch(primaryKeyValue);
    }



    public int compareTo(Object a , Object b) throws DBAppException {
        if(a instanceof Integer && b instanceof Integer)
        {
            Integer x=(Integer) a;
            Integer y=(Integer) b;
            return x.compareTo(y);

        }
        else if(a instanceof Double && b instanceof Double)
        {
            Double x=(Double) a;
            Double y=(Double) b;
            return x.compareTo(y);


        }
        else if (a instanceof String && b instanceof String)
        {
            String x=(String) a;
            String y=(String) b;
            return (x.compareTo(y));

        }
        else if (a instanceof Date && b instanceof Date)
        {
            Date x=(Date) a;
            Date y=(Date) b;
            return x.compareTo(y);

        }
        else
            throw new DBAppException();

    }
    private int genericTupleSearch(Object primaryKey) throws DBAppException {
        int left = 0;
        int right = currentSize - 1;
        int mid=0;
        Object value=null;
        while (left <= right) {

            mid = (left + right) / 2;

             value=allRecords.get(mid).getTuplePrimaryKeyValue();

            if (value == primaryKey) {
                return mid;
                //throw new DBAppException();  //the key is a duplicate
            } else if (Compare.compareTo(primaryKey,value)>0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }



        if(allRecords.size()>0)
            value=allRecords.get(mid).getTuplePrimaryKeyValue();

        if(allRecords.size()>0 && compareTo(primaryKey,value)<0)
            return mid;

        else if (allRecords.size()>0 && compareTo(primaryKey,value)>0)
            return mid+1 ;

        return mid;



    }
    public void printPage()
    {
        System.out.println("PAGE ID "+pageId+" PAGE MIN "+minKey+" PAGE MAX "+maxKey+" PAGE NUMBER OF TUPLES "+currentSize);
        for(int i=0;i<allRecords.size();i++)
        {
            Tuple tuple=allRecords.get(i);
            tuple.printTuple();
        }
    }



    public static Page deserializePage(String tableName,Integer pageId)
    {
        Page page=null;

        try {
            FileInputStream fileIn = new FileInputStream("tablesData"+File.separator+tableName+File.separator+pageId+".ser");
            ObjectInputStream in =new ObjectInputStream(fileIn);
            page=(Page)in.readObject();
            in.close();
            fileIn.close();
        }
        catch(Exception e)
        {
            System.out.println("ERROR IN deserialize METHOD IN PAGE CLASS "+ pageId);
        }
        return page;

    }


    public void serializePage(String tableName)
    {
        try {

            FileOutputStream fileOut = new FileOutputStream("tablesData"+File.separator+tableName+File.separator+pageId+".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        }
        catch (Exception e)
        {
            System.out.println("ERROR IN SERIALIZE PAGE METHOD IN CLASS PAGE");
            e.printStackTrace();
        }


    }





    //new code!!





    public void deleteRecord(Tuple tuple) throws DBAppException, IOException {

        Object primaryKeyValue = tuple.getTuplePrimaryKeyValue();
        //i got the value of the primary key to know where to delete the record


        //get the position the tuple that must be deleted
        int indexOfTuple=getTupleIndex(primaryKeyValue);

        if(indexOfTuple>=allRecords.size())
            throw new DBAppException("THE CLUSTERING KEY "+primaryKeyValue+" DOES NOT EXIST !!!");

        Tuple tupleToBeDeleted=allRecords.get(indexOfTuple);  //got the tuple that supposedly must be deleted

        //the primary key does not exist


        if(!tupleToBeDeleted.getTuplePrimaryKeyValue().equals(tuple.getTuplePrimaryKeyValue()) || !tupleToBeDeleted.equalAllValues(tuple))
        {
            throw new DBAppException("THE RECORD WITH KEY = "+tuple.getTuplePrimaryKeyValue()+" CAN NOT BE DELETED!!");
        }



        allRecords.remove(tupleToBeDeleted);
        Octree.deleteFromAllOctrees(tuple.tableName, tupleToBeDeleted,tupleToBeDeleted.getTuplePrimaryKeyValue());//delete this record from all octrees
        currentSize--;

        //update the min and max keys!
        if(allRecords.size()>0)
        {
            minKey=allRecords.get(0).getTuplePrimaryKeyValue();
            maxKey=allRecords.get(allRecords.size()-1).getTuplePrimaryKeyValue();
        }
    }


    public void deleteRecord(Hashtable<String,Object> record) throws DBAppException, IOException {
        for(int i=0;i<allRecords.size();i++)
        {
            Tuple tuple = allRecords.get(i);

            if(tuple.equalAllValues(record))
            {
                allRecords.remove(i);
                Octree.deleteFromAllOctrees(tuple.tableName,tuple,tuple.getTuplePrimaryKeyValue()); //delete this record from all octrees
                                                                                     // by searching then checking leaf has same primary key
                //System.out.println("delete method in page");
                i--;
            }

            //update the min and max keys and current size!
            if(allRecords.size()>0)
            {
                minKey=allRecords.get(0).getTuplePrimaryKeyValue();
                maxKey=allRecords.get(allRecords.size()-1).getTuplePrimaryKeyValue();
                currentSize=allRecords.size();
            }

        }


    }


    public Tuple updatePage(Object primaryKeyValue , Hashtable<String,Object> updatedRecord) throws DBAppException {

        int indexOfTuple = getTupleIndex(primaryKeyValue);

        if(indexOfTuple>=allRecords.size())
            throw new DBAppException("THE CLUSTERING KEY "+primaryKeyValue+" DOES NOT EXIST !!!");

        Tuple tupleToBeUpdated = allRecords.get(indexOfTuple);

        return tupleToBeUpdated.updateTuple(primaryKeyValue,updatedRecord);


    }





    public void seqScanAllAnd(ArrayList<String> columnName , ArrayList<Object> columnValue,ArrayList<String>operators,Vector<Tuple> resultSet) throws DBAppException //only called if resultSet is empty
    {
        for(int i=0;i<allRecords.size();i++)
        {
            Tuple tuple = allRecords.get(i);
            if(tuple.allAnd(columnName , columnValue , operators))
                resultSet.add(tuple);
        }
    }

    public void seqScanAllOr(ArrayList<String> columnName , ArrayList<Object> columnValue,ArrayList<String>operators,Vector<Tuple> resultSet) throws DBAppException {
        for(int i=0;i<allRecords.size();i++)
        {
            Tuple tuple = allRecords.get(i);
            if(tuple.allOr(columnName, columnValue,operators))
                resultSet.add(tuple);

        }
    }

    public void seqScanAllXor(ArrayList<String> columnName , ArrayList<Object> columnValue,ArrayList<String>operators,Vector<Tuple> resultSet) throws DBAppException {
        for(int i=0;i<allRecords.size();i++)
        {
            Tuple tuple =allRecords.get(i);
            boolean flag=false;
            if(tuple.allXor(columnName, columnValue,operators)) //satisfies these xor conditions
            {
                //if it is in my result set remove it else add it
                for(int j=0;j<resultSet.size();j++)
                {
                   Tuple tupleInResultSet=resultSet.get(j);
                   if(tuple.equalAllValues(tupleInResultSet))
                   {
                       flag=true;
                       resultSet.remove(j);
                   }
                }
                if(!flag)
                    resultSet.add(tuple);



            }
        }
    }




}
