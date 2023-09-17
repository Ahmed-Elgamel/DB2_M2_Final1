import java.util.Hashtable;

public class testProject6 {
    public static void main(String[] args) {




        CSVWriter.deleteAllCreatedFiles();

        DBApp dbApp = new DBApp( );

        try {
            String strTableName = "Student";

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.double");
            htblColNameType.put("birthdate", "java.util.date");

            Hashtable htblColNameMin = new Hashtable();
            htblColNameMin.put("id", "0");
            htblColNameMin.put("name", "A");
            htblColNameMin.put("gpa", "0.01");
            htblColNameMin.put("birthdate", "1900-1-1");

            Hashtable htblColNameMax = new Hashtable();
            htblColNameMax.put("id", "20000");
            htblColNameMax.put("name", "Zz");
            htblColNameMax.put("gpa", "9.99");
            htblColNameMax.put("birthdate", "2023-12-12");

            dbApp.createTable(strTableName, "id", htblColNameType, htblColNameMin, htblColNameMax);



            Hashtable htblColNameValue = new Hashtable();


            htblColNameValue.clear();
            htblColNameValue.put("id", (1));
            //htblColNameValue.put("name", new String("Z"));
            //htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);



            htblColNameValue.clear();
            htblColNameValue.put("id", (3));
            //htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);



            htblColNameValue.clear();
            htblColNameValue.put("id", (4));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);



            htblColNameValue.clear();
            htblColNameValue.put("id", (6));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);



            htblColNameValue.clear();
            htblColNameValue.put("id", (8));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);



            htblColNameValue.clear();
            htblColNameValue.put("id", (13));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);





            htblColNameValue.clear();
            htblColNameValue.put("id", (16));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);




            htblColNameValue.clear();
            htblColNameValue.put("id", (23));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);



            htblColNameValue.clear();
            htblColNameValue.put("id", (45));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);



            htblColNameValue.clear();
            htblColNameValue.put("id", (12));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);


            htblColNameValue.clear();
            htblColNameValue.put("id", (15));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);



            htblColNameValue.clear();
            htblColNameValue.put("id", (15));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.deleteFromTable(strTableName, htblColNameValue);


            htblColNameValue.clear();
            htblColNameValue.put("id", (12));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.deleteFromTable(strTableName, htblColNameValue);


            dbApp.printTable(strTableName);
            htblColNameValue.clear();
            htblColNameValue.put("id", (12));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);




            htblColNameValue.clear();
            htblColNameValue.put("id", (47));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            dbApp.insertIntoTable(strTableName, htblColNameValue);

            dbApp.printTable(strTableName);

//            htblColNameValue.clear();
//            htblColNameValue.put("gpa", (1.25));
//            htblColNameValue.put("name", ("Z"));
//            htblColNameValue.put("id", (16));
//             dbApp.deleteFromTable(strTableName, htblColNameValue);


            htblColNameValue.clear();
            htblColNameValue.put("gpa", (new DBAppNull(null)));
            htblColNameValue.put("name", ("ZZ"));
            //htblColNameValue.put("id", (1600));
             dbApp.updateTable(strTableName, "47",htblColNameValue);

            dbApp.printTable(strTableName);


                        htblColNameValue.clear();
            //htblColNameValue.put("gpa", (new Null(null)));
            //htblColNameValue.put("name", ("Z"));
            htblColNameValue.put("id", (47));
             dbApp.deleteFromTable(strTableName, htblColNameValue);

             dbApp.printTable(strTableName);




        }
        catch (Exception e){
            e.printStackTrace();
        }






    }
}
