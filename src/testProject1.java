import java.util.Hashtable;

public class testProject1 {


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
            htblColNameMin.put("id", "1");
            htblColNameMin.put("name", "a");
            htblColNameMin.put("gpa", "0.01");

            Hashtable htblColNameMax = new Hashtable();
            htblColNameMax.put("id", "20");
            htblColNameMax.put("name", "z");
            htblColNameMax.put("gpa", "9.99");

            dbApp.createTable(strTableName, "name", htblColNameType, htblColNameMin, htblColNameMax);



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
            htblColNameValue.put("name", new String("a"));
            //htblColNameValue.put("gpa",  ( 1.25 ) );
            dbApp.insertIntoTable(strTableName, htblColNameValue);
            //dbApp.printTable(strTableName);


        htblColNameValue.clear( );
        htblColNameValue.put("id", ( 13 ));
        htblColNameValue.put("name", new String("az" ) );
        htblColNameValue.put("gpa",  ( 1.25 ) );
        dbApp.insertIntoTable( strTableName , htblColNameValue );
        //dbApp.printTable(strTableName);



        htblColNameValue.clear( );
        htblColNameValue.put("id", ( 11 ));
        htblColNameValue.put("name", new String("john Noor" ) );
        htblColNameValue.put("gpa", ( 1.25 ) );
        dbApp.insertIntoTable( strTableName , htblColNameValue );
        //dbApp.printTable(strTableName);


        htblColNameValue.clear( );
        htblColNameValue.put("id", ( 10 ));
        htblColNameValue.put("name", new String("yzaky Noor" ) );
        htblColNameValue.put("gpa", ( 1.25 ) );
        dbApp.insertIntoTable( strTableName , htblColNameValue );
        //dbApp.printTable(strTableName);



            dbApp.printTable(strTableName);
            htblColNameValue.clear();
            htblColNameValue.put("id", (2));
            //htblColNameValue.put("name", new String("ZZZZZZZZZ" ) );
            // htblColNameValue.put("gpa", ( 1.25 ) );
             dbApp.updateTable(strTableName, "z", htblColNameValue);

            dbApp.printTable(strTableName);

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }






    }
}
