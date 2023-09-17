import java.util.Hashtable;

public class testOctree2 {

    public static void main(String[] args) {

        CSVWriter.deleteAllCreatedFiles();

        DBApp dbApp = new DBApp( );

        try {

            String strTableName = "Student";

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("age", "java.lang.Integer");
            htblColNameType.put("gpa", "java.lang.double");

            Hashtable htblColNameMin = new Hashtable();
            htblColNameMin.put("id", "1");
            htblColNameMin.put("age", "1");
            htblColNameMin.put("gpa", "0.01");

            Hashtable htblColNameMax = new Hashtable();
            htblColNameMax.put("id", "20");
            htblColNameMax.put("age", "100");
            htblColNameMax.put("gpa", "9.99");

            dbApp.createTable(strTableName, "age", htblColNameType, htblColNameMin, htblColNameMax);



            Hashtable htblColNameValue = new Hashtable();


            strTableName = "Student";
            htblColNameValue.clear();
            htblColNameValue.put("id", (20));
            htblColNameValue.put("age", 100);
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);


            htblColNameValue.clear();
            htblColNameValue.put("id", ( 10 ));
            htblColNameValue.put("age", 1);
            //htblColNameValue.put("gpa",  ( 1.25 ) );
            dbApp.insertIntoTable(strTableName, htblColNameValue);


            htblColNameValue.clear( );
            htblColNameValue.put("id", ( 13 ));
            htblColNameValue.put("age", 2 );
            htblColNameValue.put("gpa",  ( 1.25 ) );
            dbApp.insertIntoTable( strTableName , htblColNameValue );



            htblColNameValue.clear( );
            htblColNameValue.put("id", ( 11 ));
            htblColNameValue.put("age", 3);
            htblColNameValue.put("gpa", ( 1.25 ) );
            dbApp.insertIntoTable( strTableName , htblColNameValue );


            htblColNameValue.clear( );
            htblColNameValue.put("id", ( 10 ));
            htblColNameValue.put("age", 4 );
            htblColNameValue.put("gpa", ( 1.25 ) );
            dbApp.insertIntoTable( strTableName , htblColNameValue );



            dbApp.printTable(strTableName);
            System.out.println("---------------------------------------------------------------------");

            htblColNameValue.clear();
            htblColNameValue.put("gpa", 1.25);
            //htblColNameValue.put("name", new String("ZZZZZZZZZ" ) );
            // htblColNameValue.put("gpa", ( 1.25 ) );
            //dbApp.updateTable(strTableName, "z", htblColNameValue);
            dbApp.createIndex(strTableName,"octree1","age","gpa","id");

            dbApp.deleteFromTable(strTableName,htblColNameValue);

            dbApp.printTable(strTableName);
            dbApp.printOctree("octree1");

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }



    }
}
