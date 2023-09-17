import java.io.*;
import java.util.*;

public class CSVWriter {

    public CSVWriter()
    {

    }
    public CSVWriter(String[][] csv, String s) {
        write2DArrayToCsv(s,csv,true);

    }

    public static void initializeMetaDataHeader(String filePath)
    {
        String [][] arr=new String[2][8];
        arr[1]=new String[] {"Table Name", "Column Name", "Column Type", "ClusteringKey", "IndexName","IndexType", "min", "max"};
        //write2DArrayToCsv(filePath,arr);


        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            File file = new File(filePath);


            for (int i = 1; i < arr.length; i++) {
                for (int j = 0; j < arr[i].length; j++) {
                    writer.write(arr[i][j]);
                    if (j != arr[i].length - 1) {
                        writer.write(",");
                    }
                }
                writer.newLine();
            }


            //System.out.println("Data appended to file from line " + startLine);
        } catch (IOException e) {
            System.err.println("Error appending data to file: " + e.getMessage());
        }

    }



    public static String[][] createCsv(String strTableName, String strClusteringKeyColumn,
                                Hashtable<String,String> htblColNameType,
                                Hashtable<String,String> htblColNameMin,
                                Hashtable<String,String> htblColNameMax )
    {


        int rowNum=htblColNameMax.size();
        rowNum++;
        String [][] arr=new String[rowNum][8];

        arr[0]=new String[] {"Table Name", "Column Name", "Column Type", "ClusteringKey", "IndexName","IndexType", "min", "max"};

        // Storing all entries of Hashtable
        // in a Set using entrySet() method

        Set<Map.Entry<String, String>> entrySet = htblColNameMax.entrySet();

        // Iterating through the Hashtable object
        // using for-each loop
        int k=1;
        // System.out.println(Arrays.deepToString(entrySet.toArray()));
        for (Map.Entry<String, String> entry : entrySet)
        {

            String key = entry.getKey();

            arr[k][0]=strTableName;  //first column of each row contains table name
            arr [k][1]=key; //second column of each row contains column name
            k++;
        }

        for(int i=1;i<rowNum;i++)
        {
            String columnName=arr[i][1];
            arr[i][2]=htblColNameType.get(columnName);

            if(columnName.equals(strClusteringKeyColumn))
                arr[i][3]="true";
            else
                arr[i][3]="false";

            arr[i][4]="null";
            arr[i][5]="null";
            arr[i][6]=htblColNameMin.get(columnName);
            arr[i][7]=htblColNameMax.get(columnName);
        }

        return  arr;



    }




    public static void write2DArrayToCsv(String filePath, String[][] data,boolean append) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, append))) {
            // Get the starting line number for writing
            int startLine = 1; // Default to 1 if file is empty
            File file = new File(filePath);
            if (file.length() != 0) { // If the file is not empty
                startLine = writer.toString().split("\n").length;
            }
            // Write each row of data to the file
            //incremented the i to ignore the table header
            for (int i = 1; i < data.length; i++) {
                for (int j = 0; j < data[i].length; j++) {
                    writer.write(data[i][j]);
                    if (j != data[i].length - 1) {
                        writer.write(",");
                    }
                }
                writer.newLine();
            }


            //System.out.println("Data appended to file from line " + startLine);
        } catch (IOException e) {
            System.err.println("Error appending data to file: " + e.getMessage());
        }
    }


    public static void makeDirectory(String subDirectoryName,String parentDirectoryName)
    {
        File parentDirectory = new File(parentDirectoryName);
        File subDirectory = new File(parentDirectory, subDirectoryName);


        if (!parentDirectory.exists()) {
            parentDirectory.mkdirs();
        }

        if (!subDirectory.exists()) {
            boolean created = subDirectory.mkdir();
            if (created) {
                //System.out.println("Directory " + subDirectoryName + " was created inside " + parentDirectoryName);
            } else {
                //System.out.println("Directory " + subDirectoryName + " could not be created inside " + parentDirectoryName);
            }
        } else {
            // System.out.println("Directory " + subDirectoryName + " already exists inside " + parentDirectoryName);
        }
    }

    public static void makeDirectory(String directoryName)
    {
        File directory = new File(directoryName);

        if (!directory.exists()) {
            directory.mkdirs();
        }


    }

    public static void deleteAllCreatedFiles()
    {
        File metadataFile= new File("src"+File.separator+"resource"+File.separator+"metadata.csv");
        metadataFile.delete();

        File directory1=new File("tablesData");
        File directory2=new File("tables");
        File directory3=new File("Indices");

        File[] files1 = directory1.listFiles();
        File[] files2 = directory2.listFiles();
        File[] files3 = directory3.listFiles();

        if(files1!=null)
        {
            for(File file : files1)
            {
                if(file.isDirectory())
                {
                    deleteAllFiles(file);
                    file.delete();
                }

            }
            directory1.delete();
        }


        if(files2!=null)
        {
            for(File file : files2)
            {
                file.delete();
            }
            directory2.delete();
        }

        if(files3!=null)
        {
            for(File file : files3)
            {
                file.delete();
            }
            directory3.delete();
        }
    }
    private static void deleteAllFiles(File directory)
    {
        File []files=directory.listFiles();

        for(File file : files)
        {
            if(file.isFile())
                file.delete();
        }
    }



    //NEW CODE

    //read all csv file
    public static String[][] readCSV(String filePath) throws IOException {
        List<String[]> rows = new ArrayList<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] cells = line.split(",");
                rows.add(cells);
            }
        }

        int numRows = rows.size();
        int numColumns = rows.isEmpty() ? 0 : rows.get(0).length;
        String[][] csvData = new String[numRows][numColumns];

        for (int i = 0; i < numRows; i++) {
            csvData[i] = rows.get(i);
        }

        return csvData;
    }


    //update csv file for the constructed index
    public static void updateCsv (String tableName ,String indexName, String column1 ,String column2 ,String column3) throws IOException {
        String path ="src"+File.separator+"resource"+File.separator+"metadata.csv";
        String [][]csv=readCSV(path);

        for(int i=0;i<csv.length;i++)
        {
            for(int j=0;j<csv[i].length;j++)
            {
                if( csv[i][0].equals(tableName) && (csv[i][1].equals(column1) || csv[i][1].equals(column2) || csv[i][1].equals(column3)) )
                {
                    csv[i][4]=indexName;
                    csv[i][5]="octree";
                }

            }
        }

        //write the whole updated csv again
        initializeMetaDataHeader(path);
        write2DArrayToCsv(path,csv,true);


    }









    public static void initializeIndexMetaDataHeader(String filePath)
    {
        String [][] arr=new String[1][5];
        arr[0]=new String[] {"Table Name", "Column Name", "IndexName","IndexType","dimension"};


        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
            File file = new File(filePath);


            for (int i = 0; i < arr.length; i++) {
                for (int j = 0; j < arr[i].length; j++) {
                    writer.write(arr[i][j]);
                    if (j != arr[i].length - 1) {
                        writer.write(",");
                    }
                }
                writer.newLine();
            }


            //System.out.println("Data appended to file from line " + startLine);
        } catch (IOException e) {
            System.err.println("Error appending data to file: " + e.getMessage());
        }
    }


        public static void createIndexMetaData(String tableName , String indexName ,String column1 , String column2 , String column3) throws IOException {
        Map<String, ColumnMetadata> columnMetadataMap = CsvReader.readCsvFile(tableName);

        ColumnMetadata columnMetadata1=columnMetadataMap.get(column1);
        ColumnMetadata columnMetadata2=columnMetadataMap.get(column2);
        ColumnMetadata columnMetadata3=columnMetadataMap.get(column3);

        String [][] data = new String[4][5];
        data[0]=new String[]{"","","","",""};
        data[1]=new String[]{tableName,column1,indexName,"octree","x"};
        data[2]=new String[]{tableName,column2,indexName,"octree","y"};
        data[3]=new String[]{tableName,column3,indexName,"octree","z"};


            String path ="src"+File.separator+"resource"+File.separator+"IndicesMetaData.csv";
            CSVWriter.write2DArrayToCsv(path,data,true);


        }


}