import java.text.SimpleDateFormat;
import java.util.Date;

public  class ColumnMetadata {
    private final String tableName;
    private final String columnName;
    private final String columnType;
    private final boolean isClusteringKey;
    private final String indexName;
    private final String indexType;
    private final String min;
    private final String max;


    // Column metadata class to hold column information


    public ColumnMetadata(String tableName, String columnName, String columnType, boolean isClusteringKey,
                          String indexName, String indexType, String min, String max) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.columnType = columnType;
        this.isClusteringKey = isClusteringKey;
        this.indexName = indexName;
        this.indexType = indexType;
        this.min = min;
        this.max = max;
    }

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public boolean isClusteringKey() {
        return isClusteringKey;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public String getMin() {
        return min;
    }

    public String getMax() {
        return max;
    }

    public Object getMinParsed() throws DBAppException {
        Object value=null;
        if(columnType.equalsIgnoreCase(Integer.class.getName()))
            value= Integer.parseInt(min);

        if(columnType.equalsIgnoreCase(String.class.getName()))
            value= min;

        if(columnType.equalsIgnoreCase(Double.class.getName()))
            value= Double.parseDouble(min);

        if(columnType.equalsIgnoreCase(Date.class.getName()))
        try {
            value= new SimpleDateFormat("yyyy-MM-dd").parse(min);

        } catch (Exception e) {
            System.out.println("DATE FORMAT ERROR!!");
        }
        if(value==null)
            throw new DBAppException("MIN "+ min +" CAN NOT BE PARSED");
        return value;
    }
    public Object getMaxParsed() throws DBAppException {

        Object value=null;
        if(columnType.equalsIgnoreCase(Integer.class.getName()))
            value= Integer.parseInt(max);

        if(columnType.equalsIgnoreCase(String.class.getName()))
            value= max;
        if(columnType.equalsIgnoreCase(Double.class.getName()))
            value= Double.parseDouble(max);

        if(columnType.equalsIgnoreCase(Date.class.getName()))
            try {
                value= new SimpleDateFormat("yyyy-MM-dd").parse(max);

            } catch (Exception e) {
                System.out.println("DATE FORMAT ERROR!!");
            }
        if(value==null)
            throw new DBAppException("MAX "+ max +" CAN NOT BE PARSED");

        return value;

    }
}