import java.util.Hashtable;

public class testProject3 {

    public static void main(String[] args) {

        CSVWriter.deleteAllCreatedFiles();

        DBApp dbApp = new DBApp( );

        try {
            String strTableName = "Student";

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.double");

            Hashtable htblColNameMin = new Hashtable();
            htblColNameMin.put("id", "0");
            htblColNameMin.put("name", "A");
            htblColNameMin.put("gpa", "0.01");

            Hashtable htblColNameMax = new Hashtable();
            htblColNameMax.put("id", "20000");
            htblColNameMax.put("name", "ZzZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ");
            htblColNameMax.put("gpa", "9.99");

            dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);


            Hashtable htblColNameValue = new Hashtable();


            strTableName = "Student";
            htblColNameValue.clear();
            htblColNameValue.put("id", (20));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);
            //dbApp.printTable(strTableName);


            htblColNameValue.clear();
            htblColNameValue.put("id", ( 10 ));
            //htblColNameValue.put("name", new String("a"));
            //htblColNameValue.put("gpa",  ( 1.25 ) );
            dbApp.insertIntoTable(strTableName, htblColNameValue);
            //dbApp.printTable(strTableName);


        htblColNameValue.clear( );
        htblColNameValue.put("id", ( 900 ));
        htblColNameValue.put("name", new String("Z" ) );
        htblColNameValue.put("gpa",  ( 1.205 ) );
        dbApp.insertIntoTable( strTableName , htblColNameValue );
        //dbApp.printTable(strTableName);



        htblColNameValue.clear( );
        htblColNameValue.put("id", ( 11 ));
        htblColNameValue.put("name", new String("John Noor" ) );
        htblColNameValue.put("gpa", ( 1.25 ) );
        dbApp.insertIntoTable( strTableName , htblColNameValue );
        //dbApp.printTable(strTableName);


        htblColNameValue.clear( );
        htblColNameValue.put("id", ( 0 ));
        htblColNameValue.put("name", new String("Zaky Noor" ) );
        htblColNameValue.put("gpa", ( 1.25 ) );
        dbApp.insertIntoTable( strTableName , htblColNameValue );
        //dbApp.printTable(strTableName);


            htblColNameValue.clear();
            //htblColNameValue.put("id", (1));
            htblColNameValue.put("name", new String("ZZZZZZZZZ" ) );
            htblColNameValue.put("gpa", (9.25 ) );
            dbApp.updateTable(strTableName, "0", htblColNameValue);
            dbApp.printTable(strTableName);



            htblColNameValue.clear();
            //htblColNameValue.put("id", (1));
            htblColNameValue.put("gpa", (1.25 ) );
            dbApp.deleteFromTable(strTableName, htblColNameValue);


            dbApp.printTable(strTableName);
        }
        catch (Exception e){
            e.printStackTrace();
        }










    }
}
