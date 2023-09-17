import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;

public class testProject2 {


    public static void main(String[] args) {

        CSVWriter.deleteAllCreatedFiles();

        DBApp dbApp = new DBApp( );

        try {

            Hashtable htblColNameType = new Hashtable();
            Hashtable htblColNameMin = new Hashtable();
            Hashtable htblColNameMax = new Hashtable();
            Hashtable htblColNameValue = new Hashtable();


            String strTableName = "athlete";

            htblColNameType.clear();
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("age", "java.lang.Integer");
            htblColNameType.put("height", "java.lang.double");
            htblColNameType.put("birthdate", "java.util.Date");

            htblColNameMin.clear();
            htblColNameMin.put("name", "a");
            htblColNameMin.put("age", "1");
            htblColNameMin.put("height", "1.60");
            htblColNameMin.put("birthdate", "1980-1-1");

            htblColNameMax.clear();
            htblColNameMax.put("name", "zz");
            htblColNameMax.put("age", "30");
            htblColNameMax.put("height", "3.2");
            htblColNameMax.put("birthdate", "2020-12-30");

            dbApp.createTable(strTableName, "birthdate", htblColNameType, htblColNameMin, htblColNameMax);


            strTableName = "athlete";

            htblColNameValue.clear();
            htblColNameValue.put("name", "ahmed");
            htblColNameValue.put("age", 2);
            htblColNameValue.put("height", (1.90));

            String sDate1 = "2020-1-15";
            Date date = new SimpleDateFormat("yyyy-MM-dd").parse(sDate1);

            htblColNameValue.put("birthdate", date);
            dbApp.insertIntoTable(strTableName, htblColNameValue);
            //dbApp.printTable(strTableName);


            htblColNameValue.clear();
            htblColNameValue.put("name", "mohamed");
            htblColNameValue.put("age", 20);
            htblColNameValue.put("height", (2.0));

            sDate1 = "2019-2-2";
            date = new SimpleDateFormat("yyyy-MM-dd").parse(sDate1);

            htblColNameValue.put("birthdate", date);
            dbApp.insertIntoTable(strTableName, htblColNameValue);
            //dbApp.printTable(strTableName);


            htblColNameValue.clear();
            htblColNameValue.put("name", "youssef");
            htblColNameValue.put("age", 22);
            htblColNameValue.put("height", (3.0));

            sDate1 = "2018-2-2";
            date = new SimpleDateFormat("yyyy-MM-dd").parse(sDate1);

            htblColNameValue.put("birthdate", date);
            dbApp.insertIntoTable(strTableName, htblColNameValue);


            htblColNameValue.clear();
            htblColNameValue.put("name", "youssef");
            htblColNameValue.put("age", 22);
            htblColNameValue.put("height", (3.0));

            sDate1 = "2018-7-2";
            date = new SimpleDateFormat("yyyy-MM-dd").parse(sDate1);

            htblColNameValue.put("birthdate", date);
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            dbApp.printTable(strTableName);
        }
        catch (Exception e){
            e.printStackTrace();
        }





    }
}
