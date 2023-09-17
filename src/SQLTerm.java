import java.io.IOException;
import java.util.*;

public class SQLTerm {

    String _strTableName;
    String _strColumnName;
    String _strOperator;
    Object _objValue;

    public SQLTerm(String _strTableName, String _strColumnName, String _strOperator, Object _objValue) {
        this._strTableName = _strTableName;
        this._strColumnName = _strColumnName;
        this._strOperator = _strOperator;
        this._objValue = _objValue;
    }
    public SQLTerm()
    {

    }



    public static int getFarthestContinuousXor(int start,String []strarrOperators)
    {
        if(!strarrOperators[start].equalsIgnoreCase("xor"))
            return -1;

        int t=0;
        for(int i=start;i<strarrOperators.length;i++)
        {
            if(strarrOperators[i].equalsIgnoreCase("xor"))
                t++;
            else
                break;
        }

        return start+t;
    }


    public static int getFarthestContinuousOr(int start,String []strarrOperators)
    {
        if(!strarrOperators[start].equalsIgnoreCase("or"))
            return -1;

        int t=0;
        for(int i=start;i<strarrOperators.length;i++)
        {
            if(strarrOperators[i].equalsIgnoreCase("or"))
                t++;
            else
                break;
        }

        return start+t;
    }



        public static int getFarthestContinuousAnd(int start,String []strarrOperators)
    {
        if(!strarrOperators[start].equalsIgnoreCase("and"))
            return -1;

        int t=0;
        //System.out.println(Arrays.deepToString(strarrOperators));
        for(int i=start;i<strarrOperators.length;i++)
        {
            if(strarrOperators[i].equalsIgnoreCase("and"))
                t++;
            else
                break;
        }


        return start+t;
    }

    public  static Map<String ,ArrayList<Integer>> geIndicesOfColumns(int start,int end , String []columnNames,String tableName) throws IOException {
        Map<String ,ArrayList<String >> allOctrees = CsvReader.getAllOctrees(tableName);

        Map<String , ArrayList<Integer>> ans=new HashMap<>();


        for(int i=start;i<=end;i++)
        {
            String s =columnNames[i];
            if(start>end)
                break;

            for(String octreeName : allOctrees.keySet())
            {
                ArrayList<String > columnNamesOfOctree =allOctrees.get(octreeName);
                if(columnNamesOfOctree.contains(s))
                {
                    if(ans.containsKey(octreeName))
                        ans.get(octreeName).add(i);
                    else
                    {
                        ArrayList<Integer> arr=new ArrayList<>();
                        arr.add(i);
                        ans.put(octreeName,arr);
                    }
                }
            }
            //start++;
        }
        return ans;

    }


    public static Hashtable<String,ArrayList<Integer>> bestIndex(int start, int end , String []columnNames, String tableName) throws IOException {
        Map<String ,ArrayList<Integer >> allOctrees = geIndicesOfColumns(start,end,columnNames,tableName);

        Set<String > keys =allOctrees.keySet();

        int max =0;
        Hashtable<String,ArrayList<Integer>> ans =new Hashtable<>();

        for(String key :keys)
        {
            ArrayList<Integer > columns = allOctrees.get(key);
            if(columns.size()>max)
            {
                ans.clear();
                max=columns.size();
                ArrayList<Integer> pos=new ArrayList<>();
                for(int i=0;i<columns.size();i++)
                    pos.add(columns.get(i));

                    ans.put(key,pos);
            }
        }
        return ans;

    }



    public static Vector<Tuple> filterDuplicates(Vector<Tuple> resultSet)
    {

        Vector<Tuple> filteredResultSet = new Vector<>();

        for (Tuple tuple : resultSet) {

            boolean found=false;
            for(int i=0;i<filteredResultSet.size();i++)
            {
                Tuple tupleInFilterResultSet=filteredResultSet.get(i);
                if(tuple.equalAllValues(tupleInFilterResultSet))
                    found=true;
            }
            if(!found)
                filteredResultSet.add(tuple);

        }

        return filteredResultSet;
    }




    public String toString()
    {

        return "table name "+_strTableName  + " columnName "+_strColumnName+" operator "+_strOperator+" value "+_objValue;
    }


}
