import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

public class DBApp {





    private static void  insertCoursesRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader coursesTable = new BufferedReader(new FileReader("src/resource/courses_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = coursesTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");


            int year = Integer.parseInt(fields[0].trim().substring(0, 4));
            int month = Integer.parseInt(fields[0].trim().substring(5, 7));
            int day = Integer.parseInt(fields[0].trim().substring(8));

            Date dateAdded = new Date(year - 1900, month - 1, day);

            row.put("date_added", dateAdded);

            row.put("course_id", fields[1]);
            row.put("course_name", fields[2]);
            row.put("hours", Integer.parseInt(fields[3]));

            dbApp.insertIntoTable("courses", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        coursesTable.close();
    }

    private static void  insertStudentRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader studentsTable = new BufferedReader(new FileReader("src/resource/students_table.csv"));
        String record;
        int c = limit;
        if (limit == -1) {
            c = 1;
        }

        Hashtable<String, Object> row = new Hashtable<>();
        while ((record = studentsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("id", fields[0]);
            row.put("first_name", fields[1]);
            row.put("last_name", fields[2]);

            int year = Integer.parseInt(fields[3].trim().substring(0, 4));
            int month = Integer.parseInt(fields[3].trim().substring(5, 7));
            int day = Integer.parseInt(fields[3].trim().substring(8));

            Date dob = new Date(year - 1900, month - 1, day);
            row.put("dob", dob);

            double gpa = Double.parseDouble(fields[4].trim());

            row.put("gpa", gpa);

            dbApp.insertIntoTable("students", row);
            row.clear();
            if (limit != -1) {
                c--;
            }
        }
        studentsTable.close();
    }
    private static void insertTranscriptsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader transcriptsTable = new BufferedReader(new FileReader("src/resource/transcripts_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = transcriptsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("gpa", Double.parseDouble(fields[0].trim()));
            row.put("student_id", fields[1].trim());
            row.put("course_name", fields[2].trim());

            String date = fields[3].trim();
            int year = Integer.parseInt(date.substring(0, 4));
            int month = Integer.parseInt(date.substring(5, 7));
            int day = Integer.parseInt(date.substring(8));

            Date dateUsed = new Date(year - 1900, month - 1, day);
            row.put("date_passed", dateUsed);

            dbApp.insertIntoTable("transcripts", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        transcriptsTable.close();
    }
    private static void insertPCsRecords(DBApp dbApp, int limit) throws Exception {
        BufferedReader pcsTable = new BufferedReader(new FileReader("src/resource/pcs_table.csv"));
        String record;
        Hashtable<String, Object> row = new Hashtable<>();
        int c = limit;
        if (limit == -1) {
            c = 1;
        }
        while ((record = pcsTable.readLine()) != null && c > 0) {
            String[] fields = record.split(",");

            row.put("pc_id", Integer.parseInt(fields[0].trim()));
            row.put("student_id", fields[1].trim());

            dbApp.insertIntoTable("pcs", row);
            row.clear();

            if (limit != -1) {
                c--;
            }
        }

        pcsTable.close();
    }
    private static void createTranscriptsTable(DBApp dbApp) throws Exception {
        // Double CK
        String tableName = "transcripts";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("gpa", "java.lang.Double");
        htblColNameType.put("student_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("date_passed", "java.util.Date");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("gpa", "0.7");
        minValues.put("student_id", "43-0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("date_passed", "1990-01-01");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("gpa", "5.0");
        maxValues.put("student_id", "99-9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("date_passed", "2020-12-31");

        dbApp.createTable(tableName, "gpa", htblColNameType, minValues, maxValues);
    }

    private static void createStudentTable(DBApp dbApp) throws Exception {
        // String CK
        String tableName = "students";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("id", "java.lang.String");
        htblColNameType.put("first_name", "java.lang.String");
        htblColNameType.put("last_name", "java.lang.String");
        htblColNameType.put("dob", "java.util.Date");
        htblColNameType.put("gpa", "java.lang.Double");

        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("id", "43-0000");
        minValues.put("first_name", "AAAAAA");
        minValues.put("last_name", "AAAAAA");
        minValues.put("dob", "1990-01-01");
        minValues.put("gpa", "0.7");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("id", "99-9999");
        maxValues.put("first_name", "zzzzzz");
        maxValues.put("last_name", "zzzzzz");
        maxValues.put("dob", "2000-12-31");
        maxValues.put("gpa", "5.0");

        dbApp.createTable(tableName, "id", htblColNameType, minValues, maxValues);
    }
    private static void createPCsTable(DBApp dbApp) throws Exception {
        // Integer CK
        String tableName = "pcs";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("pc_id", "java.lang.Integer");
        htblColNameType.put("student_id", "java.lang.String");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("pc_id", "0");
        minValues.put("student_id", "43-0000");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("pc_id", "20000");
        maxValues.put("student_id", "99-9999");

        dbApp.createTable(tableName, "pc_id", htblColNameType, minValues, maxValues);
    }
    private static void createCoursesTable(DBApp dbApp) throws Exception {
        // Date CK
        String tableName = "courses";

        Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
        htblColNameType.put("date_added", "java.util.Date");
        htblColNameType.put("course_id", "java.lang.String");
        htblColNameType.put("course_name", "java.lang.String");
        htblColNameType.put("hours", "java.lang.Integer");


        Hashtable<String, String> minValues = new Hashtable<>();
        minValues.put("date_added", "1901-01-01");
        minValues.put("course_id", "0000");
        minValues.put("course_name", "AAAAAA");
        minValues.put("hours", "1");

        Hashtable<String, String> maxValues = new Hashtable<>();
        maxValues.put("date_added", "2020-12-31");
        maxValues.put("course_id", "9999");
        maxValues.put("course_name", "zzzzzz");
        maxValues.put("hours", "24");

        dbApp.createTable(tableName, "date_added", htblColNameType, minValues, maxValues);

    }




    public static void main(String[]args) throws Exception {

        CSVWriter.deleteAllCreatedFiles();

        DBApp db = new DBApp();
        db.init();

        createCoursesTable(db);
       // createPCsTable(db);
        createTranscriptsTable(db);
        createStudentTable(db);
       // insertPCsRecords(db,70);
        insertTranscriptsRecords(db,70);
        insertStudentRecords(db,70);
        insertCoursesRecords(db,70);
//        db.printTable("students");



//        String table = "students";
//        Hashtable<String, Object> row = new Hashtable();
//        //row.put("id", "44-5678");
//
//                   row.put("first_name", "zaaasss");
       // row.put("gpa",(3.5));

       // Date dob = new Date(1995 - 1900, 4 - 1, 1);
//        row.put("dob", );
       // row.put("gps", 1.1);
        //db.updateTable(table,"84-2432",row);

//           db.printTable("students");

//        db.createIndex("students","testOctree","first_name","last_name","gpa");
//        db.deleteFromTable(table, row);
//        db.printOctree("testOctree");




//        SQLTerm[] arrSQLTerms;
//	        arrSQLTerms = new SQLTerm[3];
//	        arrSQLTerms[0] = new SQLTerm();
//	        arrSQLTerms[0]._strTableName = "students";
//	        arrSQLTerms[0]._strColumnName= "first_name";
//	        arrSQLTerms[0]._strOperator = "<=";
//	        arrSQLTerms[0]._objValue = "mmmmm";
//
//        arrSQLTerms[1] = new SQLTerm();
//        arrSQLTerms[1]._strTableName = "students";
//        arrSQLTerms[1]._strColumnName= "last_name";
//        arrSQLTerms[1]._strOperator = "<";
//        arrSQLTerms[1]._objValue = "kkkkk";
//
//
//        arrSQLTerms[2] = new SQLTerm();
//        arrSQLTerms[2]._strTableName = "students";
//        arrSQLTerms[2]._strColumnName= "gpa";
//        arrSQLTerms[2]._strOperator = ">";
//        arrSQLTerms[2]._objValue = 3.6;
//
//
//
//	        String[]strarrOperators = new String[2];
//            strarrOperators[0]="or";
//
//        strarrOperators[1]="and";
//
//	      Iterator resultSet= db.selectFromTable(arrSQLTerms,strarrOperators);
//          while(resultSet.hasNext()){
//              Tuple tuple = (Tuple) resultSet.next();
//              tuple.printTuple();
//          }


        Hashtable<String, Object> record = new Hashtable<String, Object>();
        record.put("id","43-2222");
        record.put("first_name", "abdallah");

        db.insertIntoTable("students",record);
        db.createIndex("students","octree1","id","first_name","last_name");
        db.printTable("students");
        System.out.println("-----------------------------------------------------------------------------------------------------------");
        db.printOctree("octree1");
        System.out.println("-----------------------------------------------------------------------------------------------------------");

        record.clear();
        record.put("last_name", "hamed");


        db.updateTable("students","43-2222",record);
        db.printTable("students");
        System.out.println("-----------------------------------------------------------------------------------------------------------");
        db.printOctree("octree1");


        db.deleteFromTable("students",record);
        db.printTable("students");
        System.out.println("bbalabablabblab-----------------------------------------------------------------------------------------------------------");
        db.printOctree("octree1");




    }




    public DBApp() {
          init();
    }








    //** required function
    public void init()
    {
        //can initialize here the max number of records a page can have
        //any other initializations can be here


        Hashtable<Object,Object> configFileKeyValuePairs =CsvReader.readAndLoadConfigFile();
        String pageMaxNumberOfRows= (String) configFileKeyValuePairs.get("MaximumRowsCountinTablePage");
        Page.setMaxSize(Integer.parseInt(pageMaxNumberOfRows));



        try {
            File file1 =new File("src"+File.separator+"resource"+File.separator+"metadata.csv");
            file1.createNewFile();
            CSVWriter.initializeMetaDataHeader(file1.getPath());

            File file2 =new File("src"+File.separator+"resource"+File.separator+"IndicesMetaData.csv");
            file2.createNewFile();
            CSVWriter.initializeIndexMetaDataHeader(file2.getPath());
        }
        catch(Exception e) {
            System.out.println("ERROR IN CREATING THE META DATA FILE!!");
        }




    }










    //required function **
    public void createTable(String strTableName, String strClusteringKeyColumn,
                            Hashtable<String,String> htblColNameType,
                            Hashtable<String,String> htblColNameMin,
                            Hashtable<String,String> htblColNameMax ) throws DBAppException {


        try{

            strTableName=strTableName.toLowerCase();
            strClusteringKeyColumn=strClusteringKeyColumn.toLowerCase();
            Hashtable<String,String>htblColNameType1=toLowerCase(htblColNameType);
            Hashtable<String,String>htblColNameMin1=toLowerCase(htblColNameMin);
            Hashtable<String,String>htblColNameMax1=toLowerCase(htblColNameMax);


            File file2 = new File("tables"+File.separator+strTableName+".ser");
            if(file2.exists())
                throw new DBAppException("TABLE "+strTableName+" ALREADY EXISTS !!");




            File file =new File("src"+File.separator+"resource"+File.separator+"metadata.csv");



            String [][] arr=CSVWriter.createCsv( strTableName,
                    strClusteringKeyColumn,
                    htblColNameType1,
                    htblColNameMin1,
                    htblColNameMax1
            );

            InputChecker.validateCsvArray(arr);
            CSVWriter.write2DArrayToCsv(file.getPath(),arr,true);


            CSVWriter.makeDirectory(strTableName,"tablesData");
            CSVWriter.makeDirectory("tables");

            Table table=new Table( strTableName, strClusteringKeyColumn);
            table.serializeTable();

        }
        catch (Exception e){
           //e.printStackTrace();
            throw new DBAppException();
        }


    }





    //required function **
    public void insertIntoTable(String strTableName,
                                Hashtable<String,Object> htblColNameValue) throws DBAppException {


        try{

            strTableName=strTableName.toLowerCase();
            Hashtable<String,Object>htblColNameValue1=toLowerCase1(htblColNameValue);


            Table table=Table.deserliazeTable(strTableName);
            if(table==null)
                throw new DBAppException("TABLE "+strTableName+" DOES NOT EXIST!!");





            InputChecker.checkClusteringKey(table,htblColNameValue1);
            InputChecker.checkAllKeys(table,htblColNameValue1,true);


            if(table!=null)
            {
                table.genericInsertIntoTable(htblColNameValue1);
            }

            table.serializeTable();

        }
        catch (Exception e){
            e.printStackTrace();
            throw new DBAppException();
        }





    }




    //required function **
    public void deleteFromTable(String strTableName, Hashtable<String,Object>htblColNameValue) throws DBAppException
    {

        try{

            strTableName=strTableName.toLowerCase();
            Hashtable<String,Object>htblColNameValue1=toLowerCase1(htblColNameValue);

            Table table=Table.deserliazeTable(strTableName);
            if(table==null)
                throw new DBAppException("TABLE "+strTableName+" DOES NOT EXIST!!");



            if(htblColNameValue1.size()!=0)
            {
                InputChecker.checkAllKeys(table, htblColNameValue1,true);

                String indexName = CsvReader.bestIndex(strTableName,htblColNameValue1); //best index to traverse to get the required records
                if(indexName!=null)
                {
                    //deletion from table will be from the index
                    table.deleteFromTableUsingIndex(indexName,htblColNameValue1);
                    //search then delete from table after checking all conditions now the table is handled now we will handle all the indices
                    //but regardless of that all indices will have the specific values also deleted
                    //and if a page is deleted all indices must be updated accordingly
                }else
                    table.deleteRecord(htblColNameValue1);

            }
            else {
                //clear all indices so all indices are just an empty leaf basically reinitialize them
                table.deleteAllRecords();
                //reinitializeAllOctrees();
            }

            table.serializeTable();


        }
        catch (Exception e){
            e.printStackTrace();
            throw new DBAppException();
        }

    }


    //required function **

    public void updateTable(String strTableName,
                            String strClusteringKeyValue, Hashtable<String,Object> htblColNameValue )
            throws DBAppException
    {

        try{

            strTableName=strTableName.toLowerCase();
            strClusteringKeyValue=strClusteringKeyValue.toLowerCase();
            Hashtable<String,Object>htblColNameValue1=toLowerCase1(htblColNameValue);

            Table table=Table.deserliazeTable(strTableName);
            if(table==null)
                throw new DBAppException("TABLE "+strTableName+" DOES NOT EXIST!!");


//            if(htblColNameValue.size()==0)
//                throw new DBAppException("THE RECORD IS EMPTY!!");


            if(htblColNameValue1.containsKey(table.clusteringColumnName))
                throw new DBAppException("CAN NOT UPDATE A CLUSTERING KEY!!");

            InputChecker.checkAllKeys(table,htblColNameValue1,false );
            Object clusteringKeyCorrectEncapsulation = InputChecker.checkClusteringKeyEncapsulatedInString(table,strClusteringKeyValue);


            String indexName =CsvReader.columnHasIndex(strTableName,table.clusteringColumnName);

            if(indexName!=null) {
                table.updateTableUsingIndex(indexName , clusteringKeyCorrectEncapsulation, htblColNameValue1);

            }else {
                table.updateTable(clusteringKeyCorrectEncapsulation, htblColNameValue1);
            }

            table.serializeTable();
        }
        catch (Exception e){
            e.printStackTrace();
            throw new DBAppException();
        }


    }





    public void printTable(String tableName) throws DBAppException {

        try{
            tableName=tableName.toLowerCase();

            Table table =Table.deserliazeTable(tableName);
            if(table==null)
                throw new DBAppException("TABLE "+tableName+" DOES NOT EXIST!!");



            table.printPaths();

            if(table!=null)
                table.printTable();

            table.serializeTable();

        }
        catch (Exception e){
            throw new DBAppException();
        }

    }

    public void printOctree(String indexName) throws DBAppException {
        Octree octree=Octree.deserliazeOctree(indexName);
        if(octree==null)
            throw new DBAppException("OCTREE " + indexName + " DOES NOT EXIT !!");
        octree.printOctree();
        octree.serializeOctree();
    }

    public static Hashtable<String,Object> toLowerCase1(Hashtable<String , Object> record)
    {
        Set<String> keys =record.keySet();
        Hashtable<String , Object> newHashtable =new Hashtable<>();
        for(String key : keys)
        {
            Object value=record.get(key);

            if(value instanceof String)
                value=((String) value).toLowerCase();

            String key1=key.toLowerCase();
            newHashtable.put(key1,value);

        }
        return newHashtable;

    }

    public static Hashtable<String,String> toLowerCase(Hashtable<String , String> record)
    {
        Set<String> keys =record.keySet();
        Hashtable<String , String> newHashtable =new Hashtable<>();

        for(String key : keys)
        {
            String value=record.get(key).toLowerCase();
            String key1=key.toLowerCase();
            newHashtable.put(key1,value);

        }

        return newHashtable;

    }

    //new code




    public void createIndex(String tableName,String indexName , String column1 , String column2 ,String column3) throws DBAppException {
        try {
            tableName=tableName.toLowerCase();
            indexName=indexName.toLowerCase();
            column1=column1.toLowerCase();
            column2=column2.toLowerCase();
            column3=column3.toLowerCase();

            InputChecker.checkIndex(tableName,indexName,column1,column2,column3);
            Octree octree =new Octree(tableName,indexName,column1,column2,column3);

            CSVWriter.makeDirectory("Indices");
            CSVWriter.updateCsv(tableName,indexName,column1,column2,column3);
            CSVWriter.createIndexMetaData(tableName,indexName,column1,column2,column3);

            octree.serializeOctree();
        } catch (Exception e) {
            e.printStackTrace();
            throw new DBAppException();
        }
    }



    public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
        try{


            Table table = Table.deserliazeTable(arrSQLTerms[0]._strTableName);
            table.serializeTable();
            if(table==null)
                throw new DBAppException("Please enter a valid table name");


            if(arrSQLTerms.length != strarrOperators.length+1)
                throw new DBAppException("Please enter valid operations");
            for(int i =0;i<arrSQLTerms.length;i++)
            {
                if(!arrSQLTerms[i]._strTableName.equalsIgnoreCase(table.tableName))
                    throw new DBAppException("INVALID TABLE NAME");

                if(!InputChecker.columnExists(table.tableName, arrSQLTerms[i]._strColumnName))
                    throw new DBAppException("INVALID COLUMN NAME");

                if(!(arrSQLTerms[i]._strOperator.equals(">") || arrSQLTerms[i]._strOperator.equals(">=")|| arrSQLTerms[i]._strOperator.equals("<")||
                        arrSQLTerms[i]._strOperator.equals("<=")|| arrSQLTerms[i]._strOperator.equals("!=")|| arrSQLTerms[i]._strOperator.equals("=")))
                    throw new DBAppException("Please enter a valid inside SQL Operator");


                arrSQLTerms[i]._strTableName=arrSQLTerms[i]._strTableName.toLowerCase();
                arrSQLTerms[i]._strColumnName=arrSQLTerms[i]._strColumnName.toLowerCase();
            }
            for(int i =0;i<strarrOperators.length;i++)
            {
                if(strarrOperators[i]==null)
                    throw new DBAppException("Please enter a valid outside SQL Operator");


                if(!(strarrOperators[i].equalsIgnoreCase("OR") || strarrOperators[i].equalsIgnoreCase("AND") ||strarrOperators[i].equalsIgnoreCase("XOR")))
                    throw new DBAppException("Please enter a valid outside SQL Operator");


                strarrOperators[i]=strarrOperators[i].toLowerCase();
            }

            table = Table.deserliazeTable(arrSQLTerms[0]._strTableName);
            Iterator<Hashtable<String,Object>> resultSet = table.selectFromTable2(arrSQLTerms,strarrOperators);
            table.serializeTable();
            return resultSet;



        }
        catch (Exception e) {
            e.printStackTrace();
            throw new DBAppException();
        }
    }




}
