import java.io.*;
import java.util.*;

public class CsvReader {
    //private static final String FILE_PATH = "metadata.csv";
    private static final String FILE_PATH  = "src"+File.separator+"resource"+File.separator+"metadata.csv";


    // Read CSV file and return a map of column name to column metadata
    public static Map<String, ColumnMetadata> readCsvFile(String inputTableName) throws IOException {
        Map<String, ColumnMetadata> columnMetadataMap = new HashMap<>();

        BufferedReader reader = null;


       // try (CSVReader reader = new CSVReaderBuilder(new FileReader(FILE_PATH)).withSkipLines(1).build()) {
           try {

               String[] header = {"Table Name", "Column Name", "Column Type", "ClusteringKey", "IndexName", "IndexType", "min", "max"};
            String[] line;
            String line2="";
            reader = new BufferedReader(new FileReader(FILE_PATH));

            while ((line2 = reader.readLine()) != null) {
                line=line2.split(",");
                String tableName = line[0];
                if(tableName.equals(inputTableName))
                {
                    String columnName = line[1];
                    String columnType = line[2];
                    boolean isClusteringKey = Boolean.parseBoolean(line[3]);
                    String indexName = line[4];
                    String indexType = line[5];
                    String min = line[6];
                    String max = line[7];

                    ColumnMetadata columnMetadata = new ColumnMetadata(tableName, columnName, columnType,
                            isClusteringKey, indexName, indexType, min, max);

                    columnMetadataMap.put(columnName, columnMetadata);
                }
            }
        }
           catch (Exception e)
           {
               e.printStackTrace();
           }
        return columnMetadataMap;
    }


    // Get metadata for a specific column in a specific table
    public static ColumnMetadata getColumnMetadata(String tableName, String columnName) throws IOException {
        Map<String, ColumnMetadata> columnMetadataMap = readCsvFile(tableName);
        if (columnMetadataMap.containsKey(columnName)) {
            return columnMetadataMap.get(columnName);
        } else {
            throw new IllegalArgumentException("Column " + columnName + " not found in table " + tableName + " in CSV file");
        }
    }

    public static Hashtable<Object , Object> readAndLoadConfigFile()
    {
        Properties properties = new Properties();
        String filename = "src"+File.separator+"resource"+File.separator+"DBApp.config";

        try(FileInputStream fileInputStream =new FileInputStream(filename)) {
            properties.load(fileInputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Set<Object> keys = properties.keySet();
        Hashtable<Object , Object> result =new Hashtable<>();
        for(Object key : keys)
        {
            Object value =properties.get(key);
            result.put(key,value);
        }
        return result;
    }


    //new code



    //I have to create a csv for the indices to know the dimensions

    public static Map<String ,ArrayList<String >> getAllOctrees(String tableName) throws IOException {
        Map<String, ColumnMetadata> columnMetadataMap = readCsvFile(tableName);
        Map<String, ArrayList<String >> result =new HashMap<>();

        Set<String > keys =columnMetadataMap.keySet();
        for(String key : keys) // key is index name
        {
            ColumnMetadata columnMetadata =columnMetadataMap.get(key);
            String indexName =columnMetadata.getIndexName();
            String indexType =columnMetadata.getIndexType();
            if(indexType.equals("octree"))
            {
                if(result.containsKey(indexName))//add column name to this index
                    result.get(indexName).add(columnMetadata.getColumnName());
                else
                {
                    ArrayList<String> octreeColumns =new ArrayList<>();
                    octreeColumns.add(columnMetadata.getColumnName());
                    result.put(indexName,octreeColumns);
                }
            }
        }

        return result;
    }

    public static String bestIndex(String tableName ,Hashtable<String ,Object> valuesAnded) throws IOException {
        Map<String, ArrayList<String >> allOctrees = getAllOctrees(tableName);

        int max=0;
        String ans =null;
        for(String indexName : allOctrees.keySet())
        {
            int temp=0;
            ArrayList<String > columnNames =allOctrees.get(indexName);
            String column1 = columnNames.get(0);
            String column2 = columnNames.get(1);
            String column3 = columnNames.get(2);
            if(valuesAnded.containsKey(column1))
                temp++;
            if(valuesAnded.containsKey(column2))
                temp++;
            if(valuesAnded.containsKey(column3))
                temp++;
            if(temp>max)
            {
                max=temp;
                ans=indexName;
            }
        }
        return ans;
    }



    //if the column has an octree returns the HASHTABLE OF INDEX NAME AND three columns else returns null
    //modified only return index name
    public static String columnHasIndex(String tableName ,String columnName) throws IOException {
        Map<String, ArrayList<String >> allOctrees ;
        allOctrees=getAllOctrees(tableName);
        Set<String > keys =allOctrees.keySet();

        for(String key : keys)
        {
            ArrayList<String> columnsInOctree =allOctrees.get(key);
            if(columnsInOctree.contains(columnName))
            {
//                Hashtable<String , ArrayList<String >> result =new Hashtable<>();
//                result.put(key,allOctrees.get(key));
                return key;
            }
        }

        return null;

    }


//    public static void insertInAllOctrees(String tableName , Hashtable<String,Object> record) throws IOException {
//        Map<String, ArrayList<String >> allOctrees ;
//        allOctrees=getAllOctrees(tableName);
//        Set<String > keys =allOctrees.keySet();
//
//        for(String key : keys)
//        {
//            ArrayList<String> columnsInOctree =allOctrees.get(key);
//           Enumeration<String> recordKeys= record.keys();
//           ArrayList<String> columnNamesAns=new ArrayList<>();
//           while(recordKeys.hasMoreElements()){
//
//               String recordColumnName=recordKeys.nextElement();
//               if(columnsInOctree.contains(recordColumnName))
//                   columnNamesAns.add(recordColumnName);
//
//           }
//           Octree octree =Octree.deserliazeOctree(key);
//           octree.insert();
//        }

//    }
}
