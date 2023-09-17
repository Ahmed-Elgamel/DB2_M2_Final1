import java.util.Hashtable;

public class testOctree3 {

    public static void main(String[] args) {
        CSVWriter.deleteAllCreatedFiles();

        DBApp dbApp = new DBApp( );

        try {

            String strTableName = "athlete";

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


            strTableName = "athlete";
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
            //htblColNameValue.put("gpa",  ( 1.25 ) );
            dbApp.insertIntoTable( strTableName , htblColNameValue );



            htblColNameValue.clear( );
            htblColNameValue.put("id", ( 11 ));
            htblColNameValue.put("age", 3);
            htblColNameValue.put("gpa", ( 1.25 ) );
            dbApp.insertIntoTable( strTableName , htblColNameValue );


            htblColNameValue.clear( );
            //htblColNameValue.put("id", ( 10 ));
            htblColNameValue.put("age", 4 );
            //htblColNameValue.put("gpa", ( 1.25 ) );
            dbApp.insertIntoTable( strTableName , htblColNameValue );



            dbApp.printTable(strTableName);


            System.out.println("---------------------------------------------------------------------");

            htblColNameValue.clear();
            htblColNameValue.put("gpa", 2.22);
            //htblColNameValue.put("name", new String("ZZZZZZZZZ" ) );
            // htblColNameValue.put("gpa", ( 1.25 ) );

            dbApp.createIndex(strTableName,"sukma","age","gpa","id");
            //dbApp.printOctree("sukma");

            dbApp.updateTable(strTableName, "1", htblColNameValue);
            dbApp.updateTable(strTableName, "100", htblColNameValue);



            dbApp.printTable(strTableName);
            dbApp.printOctree("sukma");

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

    }
}
