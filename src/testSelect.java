import java.util.Hashtable;
import java.util.Iterator;

public class testSelect {

    public static void main(String[] args) {
        CSVWriter.deleteAllCreatedFiles();

        DBApp dbApp = new DBApp( );

        try {

            String strTableName = "Student";

            Hashtable htblColNameType = new Hashtable();
            htblColNameType.put("id", "java.lang.Integer");
            htblColNameType.put("name", "java.lang.String");
            htblColNameType.put("gpa", "java.lang.double");
            htblColNameType.put("height", "java.lang.double");
            htblColNameType.put("salary", "java.lang.integer");
            htblColNameType.put("baba", "java.lang.integer");



            Hashtable htblColNameMin = new Hashtable();
            htblColNameMin.put("id", "1");
            htblColNameMin.put("name", "a");
            htblColNameMin.put("gpa", "0.01");
            htblColNameMin.put("height", "170.0");
            htblColNameMin.put("salary", "0");
            htblColNameMin.put("baba", "0");


            Hashtable htblColNameMax = new Hashtable();
            htblColNameMax.put("id", "20");
            htblColNameMax.put("name", "z");
            htblColNameMax.put("gpa", "9.99");
            htblColNameMax.put("height", "222.0");
            htblColNameMax.put("salary", "999999999");
            htblColNameMax.put("baba", "999999999");


            dbApp.createTable(strTableName, "name", htblColNameType, htblColNameMin, htblColNameMax);



            Hashtable htblColNameValue = new Hashtable();


            strTableName = "Student";
            htblColNameValue.clear();
            htblColNameValue.put("id", (20));
            htblColNameValue.put("name", new String("Z"));
            htblColNameValue.put("gpa", (1.25));
            htblColNameValue.put("height", (200.0));
            htblColNameValue.put("salary", (20000));


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



//            dbApp.printTable(strTableName);
//            htblColNameValue.clear();
//            htblColNameValue.put("id", (2));


            dbApp.printTable(strTableName);

            SQLTerm [] arrSQLTerms=new SQLTerm[7];
            for(int i=0;i<arrSQLTerms.length;i++)
                arrSQLTerms[i]=new SQLTerm();

            arrSQLTerms[0]._strTableName = "Student";
            arrSQLTerms[0]._strColumnName= "name";
            arrSQLTerms[0]._strOperator = ">";
            arrSQLTerms[0]._objValue = "y";

            arrSQLTerms[1]._strTableName = "Student";
            arrSQLTerms[1]._strColumnName= "id";
            arrSQLTerms[1]._strOperator = ">=";
            arrSQLTerms[1]._objValue =  10 ;


            arrSQLTerms[2]._strTableName = "Student";
            arrSQLTerms[2]._strColumnName= "height";
            arrSQLTerms[2]._strOperator = "=";
            arrSQLTerms[2]._objValue =  200.0 ;



            arrSQLTerms[3]._strTableName = "Student";
            arrSQLTerms[3]._strColumnName= "gpa";
            arrSQLTerms[3]._strOperator = ">";
            arrSQLTerms[3]._objValue =  1.0 ;


            arrSQLTerms[4]._strTableName = "Student";
            arrSQLTerms[4]._strColumnName= "gpa";
            arrSQLTerms[4]._strOperator = "<";
            arrSQLTerms[4]._objValue =  1.9 ;


            arrSQLTerms[5]._strTableName = "Student";
            arrSQLTerms[5]._strColumnName= "salary";
            arrSQLTerms[5]._strOperator = "!=";
            arrSQLTerms[5]._objValue =  0;


            arrSQLTerms[6]._strTableName = "Student";
            arrSQLTerms[6]._strColumnName= "salary";
            arrSQLTerms[6]._strOperator = "!=";
            arrSQLTerms[6]._objValue =  new DBAppNull(null);







            String[]strarrOperators = new String[6];
            strarrOperators[0] = "and";
            strarrOperators[1]="or";
            strarrOperators[2]="and";
            strarrOperators[3]="or";
            strarrOperators[4]="and";
            strarrOperators[5]="and";


            dbApp.createIndex(strTableName,"octreeeee","name","gpa","id");
            dbApp.createIndex(strTableName,"octreeeee2","salary","height","baba");

            //dbApp.printOctree("octreeeee");
            Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
           System.out.println("resultsSet:");
            while(resultSet.hasNext())
            {
                Tuple tuple = (Tuple) resultSet.next();
                tuple.printTuple();
            }


        }
        catch (Exception e)
        {
            e.printStackTrace();
        }






    }


}
