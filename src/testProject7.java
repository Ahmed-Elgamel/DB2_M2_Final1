import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Hashtable;

public class testProject7 {

    public static void main(String[] args) {

        try {
            CSVWriter.deleteAllCreatedFiles();

            DBApp dbApp = new DBApp();
            dbApp.init();

            String strTableName = "Student";

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("ID", "java.lang.Integer");
            htblColNameType.put("NamE", "java.lang.String");
            htblColNameType.put("gPa", "java.lang.double");

            Hashtable htblColNameMin = new Hashtable();
            htblColNameMin.put("Id", "1");
            htblColNameMin.put("naME", "a");
            htblColNameMin.put("GPA", "0.01");

            Hashtable htblColNameMax = new Hashtable();
            htblColNameMax.put("iD", "20");
            htblColNameMax.put("NaME", "z");
            htblColNameMax.put("gpA", "9.99");

            dbApp.createTable(strTableName, "NAme", htblColNameType, htblColNameMin, htblColNameMax);


            strTableName = "athlete";

            htblColNameType.clear();
            htblColNameType.put("name", "java.lang.strInG");
            htblColNameType.put("age", "java.laNg.Integer");
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

            dbApp.createTable(strTableName, "BiRthdate", htblColNameType, htblColNameMin, htblColNameMax);


            Hashtable htblColNameValue = new Hashtable();


            strTableName = "Student";
            htblColNameValue.clear();
            htblColNameValue.put("id", (20));
            htblColNameValue.put("name", new String("z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);
            //dbApp.printTable(strTableName);


            htblColNameValue.clear();
            //htblColNameValue.put("id", ( 10 ));
            htblColNameValue.put("name", new String("a"));
            //htblColNameValue.put("gpa",  ( 1.25 ) );
            dbApp.insertIntoTable(strTableName, htblColNameValue);
            //dbApp.printTable(strTableName);


            htblColNameValue.clear();
            htblColNameValue.put("id", (9));
            htblColNameValue.put("name", new String("Yz"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);
            //dbApp.printTable(strTableName);


            htblColNameValue.clear();
            htblColNameValue.put("id", (11));
            htblColNameValue.put("name", new String("John Noor"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);
            //dbApp.printTable(strTableName);


            htblColNameValue.clear();
            htblColNameValue.put("id", (1));
            htblColNameValue.put("name", new String("YZaky Noor"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);
            //dbApp.printTable(strTableName);
//
//
            htblColNameValue.clear();
            //htblColNameValue.put("id", ( 1 ));
            //htblColNameValue.put("name", new String("ZZZZZZZZZ" ) );
            htblColNameValue.put("gpa", (1.25));
            dbApp.updateTable(strTableName, "a", htblColNameValue);
            dbApp.printTable(strTableName);


            strTableName = "athlete";

            htblColNameValue.clear();
            htblColNameValue.put("name", "Zahmed");
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
            htblColNameValue.put("NamE", "balbalbalba");
            htblColNameValue.put("age", 22);
            htblColNameValue.put("height", (3.0));

//        sDate1="2018-7-2";
//        date=new SimpleDateFormat("yyyy-MM-dd").parse(sDate1);

            //htblColNameValue.put("birthdate", date);
            dbApp.updateTable(strTableName, sDate1, htblColNameValue);

            dbApp.printTable(strTableName);

        }
        catch (DBAppException | ParseException e){
            e.printStackTrace();
        }







    }
}
