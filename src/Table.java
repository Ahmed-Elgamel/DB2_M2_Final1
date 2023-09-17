import java.io.*;
import java.util.*;


public class Table implements Serializable {
    String tableName;
    String clusteringColumnName;
    Integer numberOfpages=0; //USELESS WILL BE DELETED
    Vector<String> paths;   //string path for each table ID


    public Table(String strTableName, String strClusteringKeyColumn) {

        //initialilze table name
        this.tableName = strTableName;

        //initialize table primary column name
        this.clusteringColumnName = strClusteringKeyColumn;

        //initialize the vector of pages that will page our table

        paths=new Vector<>();

    }



    public void genericInsertIntoTable(Hashtable<String, Object> record) throws DBAppException {
        // get primary key of the record based on table attribute so i know where will the record be inserted
        Object primaryKeyValue = record.get(clusteringColumnName);
        //put the record in a tuple
        //before we put it in a tuple we must create a new reference because it produces errors
        Hashtable <String,Object> newRecord=new Hashtable<>();
        Set<Map.Entry<String, Object>> entrySet = record.entrySet();
        for (Map.Entry<String, Object> entry : entrySet)
            newRecord.put(entry.getKey(),entry.getValue());


        Tuple tuple = new Tuple(newRecord, tableName, clusteringColumnName);

        if (paths.size() > 0) {
            //get the required pageId to insert the record into
            int pageId = getPageId(primaryKeyValue);


            Page page=null;
            page=page.deserializePage(tableName,pageId);


            while (true) {

                tuple = page.genericInsert(tuple);
                page.serializePage(tableName);

                if (tuple == null)
                    break;

                pageId++;
                if (pageId == paths.size()) {
                    //new page must be added!

                    Page newPage=new Page(pageId);
                    //newPage.genericInsert(tuple);              //insert the tuple in the new page
                    newPage.insertFirst(tuple);

                    //add it to the path vector
                    paths.add(newPage.getPageId(),"tablesData"+File.separator+tableName+File.separator+newPage.pageId);
                    numberOfpages++;
                    //serialize the new page
                    newPage.serializePage(tableName);

                    break;
                } else
                {
                    page =page.deserializePage(tableName,pageId);
                }


                // **IMPORTANT**
                // if tuple == null then insertion is done otherwise insertion need to happen in the page that is returned
            }

        } else {
            //first record of the table

            Page newPage=new Page(0);


            newPage.insertFirst(tuple);

            paths.add(newPage.getPageId(),"tablesData"+File.separator+tableName+File.separator+newPage.pageId);
            numberOfpages++;

            newPage.serializePage(tableName);

        }
    }

    public int getPageId(Object primaryKeyValue) throws DBAppException {

        return genericPageSearch(primaryKeyValue);

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



    private int genericPageSearch(Object primaryKeyValue) throws DBAppException {
        //binary search code
        int high = paths.size() - 1;
        int low = 0;
        int mid=0;
        while (low <= high) {
            mid = (low + high) / 2;

            Page page=null;
            page=page.deserializePage(tableName,mid);


            Object minKey =  page.getMinKey();
            Object maxKey =  page.getMaxKey();


            if (compareTo(primaryKeyValue,maxKey)>0)
                low = mid + 1;

            else if(compareTo(primaryKeyValue,minKey)<0)
                high = mid - 1;
            else
            {
                //page.validate(primaryKeyValue);  //validation must happen here such as the key i want to insert must be unique
                page.serializePage(tableName);
                return mid;
            }
            page.serializePage(tableName);

        }

        return mid;
    }












    //new Code!!!!




    public void deleteRecord(Hashtable<String,Object> record) throws DBAppException, IOException {
        Set<String> keys = record.keySet();

        //put it in another hashtable because previously this caused errors
        Hashtable <String,Object> newRecord=new Hashtable<>();
        Set<Map.Entry<String, Object>> entrySet = record.entrySet();
        for (Map.Entry<String, Object> entry : entrySet)
            newRecord.put(entry.getKey(),entry.getValue());


        if(keys.contains(clusteringColumnName) && !(record.get(clusteringColumnName) instanceof DBAppNull))
        {

            Object primaryKeyValue = record.get(clusteringColumnName);

            Tuple tuple = new Tuple(newRecord, tableName, clusteringColumnName);

            //get page id by the primary key of the tuple
            int pageId = getPageId(primaryKeyValue);

            Page page=Page.deserializePage(tableName,pageId);
            page.deleteRecord(tuple);
            page.serializePage(tableName);

            if(page.allRecords.size()==0)
            {
                deletePage(pageId);
                renamePageIds(pageId+1);

                Octree.decrementPageIdsInAllOctrees(tableName,pageId+1);

                numberOfpages--;
                paths.remove(paths.size()-1);

            }


        }
        else//I am forced to iterate over the whole table
        {

            boolean deletedArecord=false;

            for(int i=0;i<paths.size();i++)
            {
                Page page =Page.deserializePage(tableName,i);
                int oldSize=page.allRecords.size();

                page.deleteRecord(newRecord);

                int newSize=page.allRecords.size();
                //page.serializePage(tableName);

                if(newSize<oldSize)
                    deletedArecord=true;

                if(page.allRecords.size()==0)
                {
                    deletePage(i);
                    renamePageIds(i+1);

                    Octree.decrementPageIdsInAllOctrees(tableName,i+1);

                    numberOfpages--;
                    paths.remove(paths.size()-1);


                    i--;

                }
                else
                    page.serializePage(tableName);


            }

            if(!deletedArecord)
                throw new DBAppException("THERE IS NO RECORD TO DELETE FROM THE GIVEN INFORMATION!!");


        }



    }

    public void deleteAllRecords() throws DBAppException {
        int n=paths.size();

        for(int i=0;i<n;i++)
        {
            deletePage(i);
        }
        numberOfpages=0;
        paths.clear();
        //Octree.reInitializeALLOctrees();

    }



    public void deletePage(int pageId) throws DBAppException {
        File file=new File("tablesData"+File.separator+tableName+File.separator+pageId+".ser");
        boolean success=file.delete();


        if(!success)
            throw new DBAppException();

        //System.out.println("page number "+pageId+" is deleted");

    }


    public void renamePageIds(int pageId) throws DBAppException {


        for(int i=pageId;i<numberOfpages;i++)
        {
            Page page=Page.deserializePage(tableName,i);
            page.pageId--;
            page.serializePage(tableName);

            deletePage(i);
        }
    }


    public void updateTable(Object strClusteringKeyValue, Hashtable<String,Object> htblColNameValue) throws DBAppException {

        Object primaryKeyValue=(Object)strClusteringKeyValue;
        int pageId=getPageId(primaryKeyValue);


        Page page =Page.deserializePage(tableName,pageId);
        page.updatePage(primaryKeyValue,htblColNameValue);
        page.serializePage(tableName);

    }

    public static Table deserliazeTable(String tableName)
    {
        Table table=null;

        try {
            FileInputStream fileIn = new FileInputStream("tables"+File.separator+tableName+".ser");
            ObjectInputStream in =new ObjectInputStream(fileIn);
            table=(Table)in.readObject();
            in.close();
            fileIn.close();
        }
        catch(Exception e)
        {
            System.out.println("ERROR IN deserialize METHOD IN TABLE CLASS FOR TABLE NAME "+ tableName);
        }
        return table;

    }

    public void serializeTable()
    {
        try {

            FileOutputStream fileOut = new FileOutputStream("tables"+File.separator+tableName+".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        }
        catch (Exception e)
        {
            System.out.println("ERROR IN SERIALIZE TABLE METHOD IN CLASS PAGE FOR TABLE NAME "+tableName);
            e.printStackTrace();
        }
    }



    public void printTable() throws DBAppException {
        System.out.println("TABLE NAME " + tableName + " NUMBER OF PAGES " + paths.size());

        //String fileName = "tablesMetaDataFiles" + File.separator + tableName;
        String fileName = "src"+File.separator+"resource"+File.separator+"metadata.csv";

        BufferedReader reader = null;
        String line = "";



        try {

            reader = new BufferedReader(new FileReader(fileName));
            line = reader.readLine();//first line is irrelevant

            System.out.print(clusteringColumnName + "  ");
            while ((line = reader.readLine()) != null) {
                String[] row = line.split(",");
                String tableName=row[0] ;
                String columnName = row[1];

                if ( !columnName.equals(clusteringColumnName)&& tableName.equals(this.tableName) )
                    System.out.print(columnName + "  ");
            }
            reader.close();
            System.out.println();
            reader = null;
            line = "";
            //printed the column names

        } catch (Exception e) {
            e.printStackTrace();
        }


        for (int i = 0; i < paths.size(); i++) {
            //System.out.println("PAGE " + i);
            Page page =null;
            page=page.deserializePage(tableName,i);

            if(page==null)
            {
                throw new DBAppException("PAGE DESERIALIZED IS NULL");
            }
            else {
                page.printPage();

                page.serializePage(tableName);
            }
        }


    }

    public void printPaths()
    {
        for(int i=0;i<paths.size();i++)
            System.out.print(paths.get(i)+" , ");

        System.out.println();
    }


    //new code 2



    public void updateTableUsingIndex(String indexName ,Object strClusteringKeyValue, Hashtable<String,Object> htblColNameValue) throws DBAppException, IOException {
        Octree octree=Octree.deserliazeOctree(indexName);
        ArrayList<Refrence> references=null;

        if(octree.gexDimensionName().equalsIgnoreCase(clusteringColumnName))
            references=octree.search(strClusteringKeyValue,new DBAppNull(null),new DBAppNull(null));

        else if(octree.getyDimensionName().equalsIgnoreCase(clusteringColumnName))
            references=octree.search(new DBAppNull(null),strClusteringKeyValue,new DBAppNull(null));

        else if(octree.getzDimensionName().equalsIgnoreCase(clusteringColumnName))
            references=octree.search(new DBAppNull(null),new DBAppNull(null),strClusteringKeyValue);

        if(references == null || references.size() != 1)
            throw new DBAppException("CLUSTERING KEY DOES NOT EXIST IN INDEX "+octree.getName()+" !!");

        int pageId = references.get(0).pageId;
        Page page=Page.deserializePage(tableName , pageId);
        Tuple oldTuple = page.updatePage(strClusteringKeyValue,htblColNameValue);
        page.serializePage(tableName);


        System.out.println("TABLE "+tableName+" IS UPDATED USING OCTREE INDEX "+octree.getName());
        Octree.updateAllOctrees(tableName,strClusteringKeyValue,oldTuple.record,htblColNameValue,page.pageId);
        System.out.println("ALL OCTREES ARE UPDATED ACCORDINGLY:)");



    }


    public void deleteFromTableUsingIndex(String indexName,Hashtable<String,Object> record) throws DBAppException, IOException {
        Octree octree = Octree.deserliazeOctree(indexName);
        Object columnNameX=octree.gexDimensionName();
        Object columnNameY=octree.getyDimensionName();
        Object columnNameZ=octree.getzDimensionName();

        ArrayList<Refrence> references =octree.search(record.get(columnNameX),record.get(columnNameY),record.get(columnNameZ));
        octree.serializeOctree();

        //System.out.println(references);
        for(int i=0;i<references.size();i++)
        {
            Refrence refrence = references.get(i);
           // System.out.println("deserializing page "+refrence.getPageId());
            Page page=Page.deserializePage(tableName,refrence.getPageId());


            //deletion in main table
            if (!(record.get(clusteringColumnName) instanceof DBAppNull))
            {
                //delete pk from table
                Tuple tuple = new Tuple(record, tableName, clusteringColumnName);
                page.deleteRecord(tuple);
            }
            else
            {
                page.deleteRecord(record);

            }


            //delete record from all indices

            if(page.allRecords.size()==0)
            {
                int pageId =page.pageId;
                deletePage(pageId);
                renamePageIds(pageId+1);

                //decrement page ids in all indices!!
                Octree.decrementPageIdsInAllOctrees(tableName,pageId+1);

                numberOfpages--;
                paths.remove(paths.size()-1);

                for(int k=0;k<references.size();k++)
                {
                    if(references.get(k).pageId>pageId)
                        references.get(k).pageId--;
                }

            }
            else
                page.serializePage(tableName);

            System.out.println("A RECORD " +" HAS BEEN DELETED FROM TABLE" +tableName +" USING INDEX "+octree.getName() +" :)");




        }



    }









    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,String[] strarrOperators)throws DBAppException
    {
        Vector<Tuple> resultSet = new Vector<Tuple>();
        for(int i =0;i<arrSQLTerms.length;i++)
        {
            if(i==0)
            {
                for(int j = 0;j<paths.size();j++)
                {
                    Page currentPage = Page.deserializePage(tableName, j);
                    Vector<Tuple> recordsInPage = currentPage.allRecords;
                    for(int f = 0;f<recordsInPage.size();f++)
                    {
                        Tuple tuple = recordsInPage.get(f);
                        Hashtable<String, Object> currentRecord = tuple.record;
                        Object currentValue = currentRecord.get(arrSQLTerms[i]._strColumnName);
                        if(arrSQLTerms[i]._strOperator.equals(">"))
                        {
                            if(compareTo(currentValue,arrSQLTerms[i]._objValue)==1)
                                resultSet.add(tuple);
                        }
                        if(arrSQLTerms[i]._strOperator.equals(">="))
                        {
                            if(compareTo(currentValue,arrSQLTerms[i]._objValue)==1 || compareTo(currentValue,arrSQLTerms[i]._objValue)==0)
                                resultSet.add(tuple);
                        }
                        if(arrSQLTerms[i]._strOperator.equals("<="))
                        {
                            if(compareTo(currentValue,arrSQLTerms[i]._objValue)==-1 || compareTo(currentValue,arrSQLTerms[i]._objValue)==0)
                                resultSet.add(tuple);
                        }
                        if(arrSQLTerms[i]._strOperator.equals("<"))
                        {
                            if(compareTo(currentValue,arrSQLTerms[i]._objValue)==-1)
                                resultSet.add(tuple);
                        }
                        if(arrSQLTerms[i]._strOperator.equals("="))
                        {
                            if(compareTo(currentValue,arrSQLTerms[i]._objValue)==0)
                                resultSet.add(tuple);
                        }
                        if(arrSQLTerms[i]._strOperator.equals("!="))
                        {
                            if(compareTo(currentValue,arrSQLTerms[i]._objValue)==1 || compareTo(currentValue,arrSQLTerms[i]._objValue)==-1)
                                resultSet.add(tuple);
                        }
                    }
                    currentPage.serializePage(tableName);
                }
            }
            else
            {
                if(strarrOperators==null || strarrOperators.length==0 )
                    break;
                else
                {
                    String operator = strarrOperators[i-1];
                    if(operator.equals("AND"))
                    {
                        for(int j=0;j<resultSet.size();j++)
                        {
                            //Tuple tuple = resultSet.get(j);
                            Tuple tuple = resultSet.get(j);
                            Hashtable<String, Object> currentRecord = tuple.record;
                            Object currentValue = currentRecord.get(arrSQLTerms[i]._objValue);
                            if(arrSQLTerms[i]._strOperator.equals(">"))
                            {
                                if(!(compareTo(arrSQLTerms[i]._objValue,currentValue)==1))
                                    resultSet.remove(tuple);
                            }
                            if(arrSQLTerms[i]._strOperator.equals(">="))
                            {
                                if(!(compareTo(arrSQLTerms[i]._objValue,currentValue)==1 || compareTo(arrSQLTerms[i]._objValue,currentValue)==0))
                                    resultSet.remove(tuple);
                            }
                            if(arrSQLTerms[i]._strOperator.equals("<="))
                            {
                                if(!(compareTo(arrSQLTerms[i]._objValue,currentValue)==-1 || compareTo(arrSQLTerms[i]._objValue,currentValue)==0))
                                    resultSet.remove(tuple);
                            }
                            if(arrSQLTerms[i]._strOperator.equals("<"))
                            {
                                if(!(compareTo(currentValue,arrSQLTerms[i]._objValue)==-1))
                                    resultSet.remove(tuple);
                            }
                            if(arrSQLTerms[i]._strOperator.equals("="))
                            {
                                if(!(compareTo(currentValue,arrSQLTerms[i]._objValue)==0))
                                    resultSet.remove(tuple);
                            }
                            if(arrSQLTerms[i]._strOperator.equals("!="))
                            {
                                if(!(compareTo(currentValue,arrSQLTerms[i]._objValue)==1 || compareTo(currentValue,arrSQLTerms[i]._objValue)==-1))
                                    resultSet.remove(tuple);
                            }
                        }
                    }
                    if(operator.equals("OR"))
                    {
                        for(int j = 0;j<paths.size();j++)
                        {
                            Page currentPage = Page.deserializePage(tableName, j);
                            Vector<Tuple> recordsInPage = currentPage.allRecords;
                            for(int f = 0;f<recordsInPage.size();f++)
                            {
                                Tuple tuple = recordsInPage.get(f);
                                Hashtable<String, Object> currentRecord = tuple.record;
                                Object currentValue = currentRecord.get(arrSQLTerms[i]._strColumnName);
                                if(arrSQLTerms[i]._strOperator.equals(">"))
                                {
                                    if(compareTo(currentValue,arrSQLTerms[i]._objValue)==1)
                                    {
                                        if(!(resultSet.contains(tuple)))
                                            resultSet.add(tuple);
                                    }
                                }
                                if(arrSQLTerms[i]._strOperator.equals(">="))
                                {
                                    if(compareTo(currentValue,arrSQLTerms[i]._objValue)==1 || compareTo(currentValue,arrSQLTerms[i]._objValue)==0)
                                    {
                                        if(!(resultSet.contains(tuple)))
                                            resultSet.add(tuple);
                                    }
                                }
                                if(arrSQLTerms[i]._strOperator.equals("<="))
                                {
                                    if(compareTo(currentValue,arrSQLTerms[i]._objValue)==-1 || compareTo(currentValue,arrSQLTerms[i]._objValue)==0)
                                    {
                                        if(!(resultSet.contains(tuple)))
                                            resultSet.add(tuple);
                                    }
                                }
                                if(arrSQLTerms[i]._strOperator.equals("<"))
                                {
                                    if(compareTo(currentValue,arrSQLTerms[i]._objValue)==-1)
                                    {
                                        if(!(resultSet.contains(tuple)))
                                            resultSet.add(tuple);
                                    }
                                }
                                if(arrSQLTerms[i]._strOperator.equals("="))
                                {
                                    if(compareTo(currentValue,arrSQLTerms[i]._objValue)==0)
                                    {
                                        if(!(resultSet.contains(tuple)))
                                            resultSet.add(tuple);
                                    }
                                }
                                if(arrSQLTerms[i]._strOperator.equals("!="))
                                {
                                    if(compareTo(currentValue,arrSQLTerms[i]._objValue)==1 || compareTo(currentValue,arrSQLTerms[i]._objValue)==-1)
                                    {
                                        if(!(resultSet.contains(tuple)))
                                            resultSet.add(tuple);
                                    }
                                }
                            }
                            currentPage.serializePage(tableName);
                        }
                    }
                    if(operator.equals("XOR"))
                    {
                        for(int j = 0;i<paths.size();j++)
                        {
                            Page currentPage = Page.deserializePage(tableName, j);
                            Vector<Tuple> recordsInPage = currentPage.allRecords;
                            for(int f = 0;f<recordsInPage.size();f++)
                            {
                                Tuple tuple = recordsInPage.get(f);
                                Hashtable<String, Object> currentRecord = tuple.record;
                                Object currentValue = currentRecord.get(arrSQLTerms[i]._strColumnName);
                                if(arrSQLTerms[i]._strOperator.equals(">"))
                                {
                                    if(compareTo(currentValue,arrSQLTerms[i]._objValue)==1)
                                    {
                                        if((resultSet.contains(tuple)))
                                            resultSet.remove(tuple);
                                        else
                                            resultSet.add(tuple);
                                    }
                                }
                                if(arrSQLTerms[i]._strOperator.equals(">="))
                                {
                                    if(compareTo(currentValue,arrSQLTerms[i]._objValue)==1 || compareTo(currentValue,arrSQLTerms[i]._objValue)==0)
                                    {
                                        if((resultSet.contains(tuple)))
                                            resultSet.remove(tuple);
                                        else
                                            resultSet.add(tuple);
                                    }
                                }
                                if(arrSQLTerms[i]._strOperator.equals("<="))
                                {
                                    if(compareTo(currentValue,arrSQLTerms[i]._objValue)==-1 || compareTo(currentValue,arrSQLTerms[i]._objValue)==0)
                                    {
                                        if((resultSet.contains(tuple)))
                                            resultSet.remove(tuple);
                                        else
                                            resultSet.add(tuple);
                                    }
                                }
                                if(arrSQLTerms[i]._strOperator.equals("<"))
                                {
                                    if(compareTo(currentValue,arrSQLTerms[i]._objValue)==-1)
                                    {
                                        if((resultSet.contains(tuple)))
                                            resultSet.remove(tuple);
                                        else
                                            resultSet.add(tuple);
                                    }
                                }
                                if(arrSQLTerms[i]._strOperator.equals("="))
                                {
                                    if(compareTo(currentValue,arrSQLTerms[i]._objValue)==0)
                                    {
                                        if((resultSet.contains(tuple)))
                                            resultSet.remove(tuple);
                                        else
                                            resultSet.add(tuple);
                                    }
                                }
                                if(arrSQLTerms[i]._strOperator.equals("!="))
                                {
                                    if(compareTo(currentValue,arrSQLTerms[i]._objValue)==1 || compareTo(currentValue,arrSQLTerms[i]._objValue)==-1)
                                    {
                                        if((resultSet.contains(tuple)))
                                            resultSet.remove(tuple);
                                        else
                                            resultSet.add(tuple);
                                    }
                                }
                            }
                            currentPage.serializePage(tableName);
                        }
                    }
                }
            }
        }
        return resultSet.iterator();
    }


    public Iterator selectFromTable2(SQLTerm[] arrSQLTerms, String[] strarrOperators)
            throws DBAppException, IOException {

        //System.out.println(Arrays.deepToString(arrSQLTerms));
        int n=arrSQLTerms.length;
        String [] columnNames=new String[n];
        for(int i=0;i<n;i++)
            columnNames[i]=arrSQLTerms[i]._strColumnName;



        int start = 0;

        Vector<Tuple> finalResultSet=new Vector<>();
        boolean modified=false;

        for(int i=0;i<strarrOperators.length;i++)
        {
            start=i;


            int end1 = SQLTerm.getFarthestContinuousAnd(start,strarrOperators);  // i think this should have a +1 for every one since i make the operation after the  operator
            int end2= SQLTerm.getFarthestContinuousOr(start,strarrOperators);
            int end3=SQLTerm.getFarthestContinuousXor(start,strarrOperators);

            if(start!=0)
                start++;
            if(end1!=-1) // there is a minimum of one and operation
            {
//                System.out.println("AND BLOCK " +start+" "+end1);
                if(!modified) {

                    Hashtable<String, ArrayList<Integer>> columnsToOctreePlACES = SQLTerm.bestIndex(start, end1, columnNames, tableName);

                    if (columnsToOctreePlACES.size() != 0) {
                        //there is a best index for this group of ands
                        //query on octree and put in result set
                        //then check other ands also that was not part of the index search

                        Enumeration<String> keys = columnsToOctreePlACES.keys();
                        String indexName = keys.nextElement();
                       // System.out.println(indexName+" is the best index to use");
                        ArrayList<Integer> places = columnsToOctreePlACES.get(indexName);
                        while(places.size()>3)
                            places.remove(places.size()-1); //i just take 3 out whatever was matched


                        ArrayList<String> columnName = new ArrayList<>();
                        ArrayList<Object> columnValues = new ArrayList<>();
                        ArrayList<String> operators = new ArrayList<>();


                        for (int l = 0; l < places.size(); l++) {//extract the index values and column names then let the octree handle it
                            int place = places.get(l);
                            columnName.add(arrSQLTerms[place]._strColumnName);
                            columnValues.add(arrSQLTerms[place]._objValue);
                            operators.add(arrSQLTerms[place]._strOperator);
                        }
//                        System.out.println("places array"+places);
//                        System.out.println("columnNames array"+columnName);
//                        System.out.println("columnValues array"+columnValues);
//                        System.out.println("operators array"+operators);



                        Octree octree = Octree.deserliazeOctree(indexName);
                        ArrayList<Refrence> references= octree.selectFromTree(columnName , columnValues,operators,1);   //type 1 for all anded
                        octree.serializeOctree();
                        addTuplesToResultSet(references,finalResultSet);

                        //then operate on this result set (remaining ands that was not in the index query)
                        columnName.clear();
                        columnValues.clear();
                        operators.clear();
                        for (int j = start; j <= end1; j++) {
                            if (!places.contains(j)) //it was not part of the index
                            {
                                //System.out.println(j+" was not part of the query on index");
                                columnName.add(arrSQLTerms[j]._strColumnName);
                                columnValues.add(arrSQLTerms[j]._objValue);
                                operators.add(arrSQLTerms[j]._strOperator);
                            }
                        }
                        filterResultSetAllAnd(columnName,columnValues,operators,finalResultSet);


                    } else if (columnsToOctreePlACES.size() == 0) {
                        //linear scan or log if clustering key (once) from start to end
                        //no index and the result set Has never been modified!!
//                        System.out.println("NO APPROPRIATE INDEX FOUND SO WE WILL DO SEQ SCAN");
                        ArrayList<String> columnName = new ArrayList<>();
                        ArrayList<Object> columnValues = new ArrayList<>();
                        ArrayList<String> operators = new ArrayList<>();
                        for (int j = start; j <= end1; j++) {
                            columnName.add(arrSQLTerms[j]._strColumnName);
                            columnValues.add(arrSQLTerms[j]._objValue);
                            operators.add(arrSQLTerms[j]._strOperator);
                        }

//                        System.out.println("columnNames array"+columnName);
//                        System.out.println("columnValues array"+columnValues);
//                        System.out.println("operators array"+operators);
                        seqScanAllAnd(columnName,columnValues,operators,finalResultSet);


                    }
                    modified=true;

                }
                else {
                    //result set is not empty(or empty!)! the point is it was modified so just perform the and operations on the nonempty result set
                   //System.out.println("WE WILL PERFORM AND OPERATION ON THE ACCUMULATED RESULT SET");
                    ArrayList<String> columnName = new ArrayList<>();
                    ArrayList<Object> columnValues = new ArrayList<>();
                    ArrayList<String> operators = new ArrayList<>();
                    for (int j = start; j <= end1; j++) {
                        columnName.add(arrSQLTerms[j]._strColumnName);
                        columnValues.add(arrSQLTerms[j]._objValue);
                        operators.add(arrSQLTerms[j]._strOperator);
                    }

                    filterResultSetAllAnd(columnName,columnValues,operators,finalResultSet);
                }


                i = end1-1; //I THINK WE SHOULD -1 BECAUSE FOR LOOP WILL INCREMENT IT
            }


            else if (end2!=-1)  //there is a minimum of at least one or operation
            {
                //System.out.println("OR BLOCK "+start+" "+end2);
                    Map<String, ArrayList<Integer>> columnsToOctreePlACES = SQLTerm.geIndicesOfColumns(start, end2, columnNames, tableName);//best index to use if there is
                    if(columnsToOctreePlACES.size()!=0)
                    {
                        ArrayList<String> columnName = new ArrayList<>();
                        ArrayList<Object> columnValues = new ArrayList<>();
                        ArrayList<String> operators = new ArrayList<>();

                        Set<String> keys = columnsToOctreePlACES.keySet();
                        ArrayList<Integer>allPlaces=new ArrayList<>();

                    for(String indexName : keys) {
                        ArrayList<Integer> places = columnsToOctreePlACES.get(indexName);

                        while(places.size()>3)
                            places.remove(places.size()-1); //i just take 3 out whatever was matched

                        for (int l = 0; l < places.size(); l++) {//extract the index values and column names then let the octree handle it
                            int place = places.get(l);
                            columnName.add(arrSQLTerms[place]._strColumnName);
                            columnValues.add(arrSQLTerms[place]._objValue);
                            operators.add(arrSQLTerms[place]._strOperator);
                            allPlaces.add(place);
                        }
//                        System.out.println("PLACES ARRAY "+places);
//                        System.out.println("columnNames array"+columnName);
//                        System.out.println("columnValues array"+columnValues);
//                        System.out.println("operators array"+operators);
                        Octree octree = Octree.deserliazeOctree(indexName);
                        ArrayList<Refrence> references = octree.selectFromTree(columnName, columnValues, operators, 2);   //type 2 for all ored
                        octree.serializeOctree();
                        //System.out.println("refrences "+references);
                        addTuplesToResultSet(references,finalResultSet);

                        columnName.clear();
                        columnValues.clear();
                        operators.clear();
                    }
                    //then add more tuples to this result set if there is remaining ORS with no index
                    for (int j = start; j <= end2; j++) {
                        if (!allPlaces.contains(j)) //it was not part of the index
                        {
//                            System.out.println(j+" was not part of the index in or operation");
                            columnName.add(arrSQLTerms[j]._strColumnName);
                            columnValues.add(arrSQLTerms[j]._objValue);
                            operators.add(arrSQLTerms[j]._strOperator);
                        }
                    }
                    seqScanAllOr(columnName,columnValues,operators,finalResultSet); //add to this result set


                }
                else  // no index found so seqScan
                {
//                    System.out.println(finalResultSet);
//                    System.out.println("no index found for or oeration");
                    ArrayList<String> columnName = new ArrayList<>();
                    ArrayList<Object> columnValues = new ArrayList<>();
                    ArrayList<String> operators = new ArrayList<>();
                    for (int j = start; j <= end2; j++) {

                        columnName.add(arrSQLTerms[j]._strColumnName);
                        columnValues.add(arrSQLTerms[j]._objValue);
                        operators.add(arrSQLTerms[j]._strOperator);
                    }
//                    System.out.println("columnNames array"+columnName);
//                    System.out.println("columnValues array"+columnValues);
//                    System.out.println("operators array"+operators);
                    seqScanAllOr(columnName,columnValues,operators,finalResultSet);
                }

                //System.out.println("or result set "+finalResultSet);

                modified=true;
                i=end2-1;

            }
            else if(end3!=-1)//must be at least one xor operation
            {
                ArrayList<String> columnName = new ArrayList<>();
                ArrayList<Object> columnValues = new ArrayList<>();
                ArrayList<String> operators = new ArrayList<>();
                for (int j = start; j <= end3; j++) {
                    columnName.add(arrSQLTerms[j]._strColumnName);
                    columnValues.add(arrSQLTerms[j]._objValue);
                    operators.add(arrSQLTerms[j]._strOperator);
                }
                seqScanAllXor(columnName,columnValues,operators,finalResultSet); //add to this result set
                i=end3-1;
                modified=true;

            }
            else
                break;


        }
        //remove duplicates first
        return SQLTerm.filterDuplicates(finalResultSet).iterator();
        //return finalResultSet.iterator();

    }


    public void addTuplesToResultSet(ArrayList<Refrence> refrences  , Vector<Tuple> resultSet) throws DBAppException {
        for(int i=0;i<refrences.size();i++)
        {
            Refrence refrence=refrences.get(i);
            int pageId=refrence.getPageId();

            Page page =  Page.deserializePage(tableName,pageId);
            Vector<Tuple> allTuplesInPage =page.getAllRecords();
            Object primaryKeyValue=refrence.getKey();
            int tupleIndex = page.getTupleIndex(primaryKeyValue); //gets the index of the tuple based on the primary key that i got from the reference
            Tuple tuple = allTuplesInPage.get(tupleIndex);
            resultSet.add(tuple);
            page.serializePage(tableName);
        }
    }

    public void filterResultSetAllAnd(ArrayList<String> columnNames , ArrayList<Object> columnValues,ArrayList<String>operators , Vector<Tuple> resultSet) throws DBAppException {
        for(int i=0;i<resultSet.size();i++)
        {
            Tuple tuple = resultSet.get(i);
            if(!tuple.allAnd(columnNames,columnValues,operators))
            {
                resultSet.remove(i);
                i--;
            }

        }
    }

    public void seqScanAllXor(ArrayList<String> columnNames , ArrayList<Object> columnValues,ArrayList<String>operators , Vector<Tuple> resultSet) throws DBAppException
    {
        for(int i=0;i<paths.size();i++)
        {
            Page page=Page.deserializePage(tableName,i);
            page.seqScanAllXor(columnNames,columnValues,operators,resultSet);
            page.serializePage(tableName);
        }
    }



    //only called if resultSet is empty
    public void seqScanAllAnd(ArrayList<String> columnNames , ArrayList<Object> columnValues,ArrayList<String>operators , Vector<Tuple> resultSet) throws DBAppException
    {
        for(int i=0;i<paths.size();i++)
        {
            Page page=Page.deserializePage(tableName,i);
            page.seqScanAllAnd(columnNames,columnValues,operators,resultSet);
            page.serializePage(tableName);
        }
    }

    public void seqScanAllOr(ArrayList<String> columnName , ArrayList<Object> columnValue,ArrayList<String>operators , Vector<Tuple> resultSet) throws DBAppException //only called if resultSet is empty
    {
        for(int i=0;i<paths.size();i++)
        {
            Page page=Page.deserializePage(tableName,i);
            page.seqScanAllOr(columnName,columnValue,operators,resultSet);
            page.serializePage(tableName);
        }
    }




}
