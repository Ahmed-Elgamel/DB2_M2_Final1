import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.Objects;

public class Point implements Serializable {
    Object x , y ,z;

    public Point(Object x , Object y , Object z)
    {
        this.x=x;
        this.y=y;
        this.z=z;
    }

    public static Point[] getEndPoints(String tableName,String column1 , String column2,String column3) throws IOException, DBAppException {
        ColumnMetadata columnMetadata1=null;
        ColumnMetadata columnMetadata2=null;
        ColumnMetadata columnMetadata3=null;

        columnMetadata1=CsvReader.getColumnMetadata(tableName,column1);
        columnMetadata2=CsvReader.getColumnMetadata(tableName,column2);
        columnMetadata3=CsvReader.getColumnMetadata(tableName,column3);

        Point[] result=new Point[2];


        result[0]= new Point(columnMetadata1.getMinParsed(), columnMetadata2.getMinParsed() , columnMetadata3.getMinParsed());
        result[1]=new Point(columnMetadata1.getMaxParsed(),columnMetadata2.getMaxParsed(),columnMetadata3.getMaxParsed());

        return result;

    }




    public static Object getMid(Object topLeftFront , Object bottomRightBack) throws DBAppException {

        if(topLeftFront instanceof Integer && bottomRightBack instanceof Integer)
        {
            Integer x=(Integer) topLeftFront;
            Integer y=(Integer) bottomRightBack;
            return (x+y)/2;

        }
        else if(topLeftFront instanceof Double && bottomRightBack instanceof Double)
        {
            Double x=(Double) topLeftFront;
            Double y=(Double) bottomRightBack;
            return (x+y)/2;

        }
        else if (topLeftFront instanceof String && bottomRightBack instanceof String)
        {
            return getMiddleString((String)topLeftFront, (String)bottomRightBack , Math.min(((String) topLeftFront).length(),((String) bottomRightBack).length()));
        }
        else if (topLeftFront instanceof Date && bottomRightBack instanceof Date)
        {
            Date x=(Date) topLeftFront;
            Date y=(Date) bottomRightBack;
            return (x.getTime()+y.getTime()) /2;

        }
        else
            throw new DBAppException("ERROR IN GET MID METHOD!!");


    }

    static String getMiddleString(String S, String T, int N)
    {
        // Stores the base 26 digits after addition
        int[] a1 = new int[N + 1];

        for (int i = 0; i < N; i++) {
            a1[i + 1] = (int)S.charAt(i) - 97
                    + (int)T.charAt(i) - 97;
        }

        // Iterate from right to left
        // and add carry to next position
        for (int i = N; i >= 1; i--) {
            a1[i - 1] += (int)a1[i] / 26;
            a1[i] %= 26;
        }

        // Reduce the number to find the middle
        // string by dividing each position by 2
        for (int i = 0; i <= N; i++) {

            // If current value is odd,
            // carry 26 to the next index value
            if ((a1[i] & 1) != 0) {

                if (i + 1 <= N) {
                    a1[i + 1] += 26;
                }
            }

            a1[i] = (int)a1[i] / 2;
        }

        StringBuilder sb =new StringBuilder("");
        for (int i = 1; i <= N; i++) {
            sb.append((char)(a1[i] + 97));

        }
        return sb.toString();
    }



    public static Object increment(Object x) throws DBAppException {

        if(x instanceof String)
            return x+"a";

        if(x instanceof Integer )
            return (Integer)x+1;

        if( x instanceof  Double)
            return (Double)x+1;

        if (x instanceof Date)
        {
            Calendar calendar=Calendar.getInstance();
            calendar.setTime((Date) x);
            calendar.add(Calendar.DATE,1);
            return calendar.getTime();
        }
        else
            throw new DBAppException("ERROR IN INCREMENT METHOD !! ");
    }





    @Override
    public boolean equals(Object obj) {
        Point point=(Point)obj;
        try {

            boolean ans =true;
            if(!(x instanceof DBAppNull) && !(point.x instanceof DBAppNull))
                ans&=Compare.compareTo(x,point.x)==0;

            if(!(y instanceof DBAppNull) && !(point.y instanceof DBAppNull))
                ans&=Compare.compareTo(y,point.y)==0;

            if(!(z instanceof DBAppNull) && !(point.z instanceof DBAppNull))
                ans&=Compare.compareTo(z,point.z)==0;


           return ans;
           // return  Compare.compareTo(x,point.x)==0 && Compare.compareTo(y,point.y)==0 && Compare.compareTo(z,point.z)==0;
        } catch (DBAppException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y, z);
    }


    public String toString() {
        return "[X = "+x+"  Y = "+y+" Z = "+z+"]";

    }
}
