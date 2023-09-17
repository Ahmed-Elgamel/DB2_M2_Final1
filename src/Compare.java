import java.util.Date;

public class Compare {

    public static int compareTo(Object a , Object b) throws DBAppException {
        if(a instanceof Integer && b instanceof Integer)
        {
            Integer x=(Integer) a;
            Integer y=(Integer) b;
            return x.compareTo(y);

        }
        else if(a instanceof Double && b instanceof Double)
        {
            Double x=(Double) a;
            Double y=(Double) b;
            return x.compareTo(y);


        }
        else if (a instanceof String && b instanceof String)
        {
            String x=(String) a;
            String y=(String) b;
            return (x.compareTo(y));

        }
        else if (a instanceof Date && b instanceof Date)
        {
            Date x=(Date) a;
            Date y=(Date) b;
            return x.compareTo(y);

        }
        else
            throw new DBAppException(a +" "+ a.getClass().getName()+" "+ b+" "+ b.getClass().getName() +" error in comparing them ");


    }

    public static boolean compareToWithOperator(Object a ,Object b ,String operator) throws DBAppException {
       if(operator.equals("!=")) {
           if (( a instanceof DBAppNull && !(b instanceof DBAppNull) ) || ( !(a instanceof DBAppNull) && b instanceof DBAppNull) ) //not sure about this!!!!!
               return true;
           else if (a instanceof DBAppNull && b instanceof DBAppNull)
               return false;

       }


        if(operator.equals("=")) {
            if (( a instanceof DBAppNull && !(b instanceof DBAppNull) ) || ( !(a instanceof DBAppNull) && b instanceof DBAppNull) ) //not sure about this!!!!!
                return false;
            else if (a instanceof DBAppNull && b instanceof DBAppNull)
                return true;

        }


        if (a instanceof DBAppNull || b instanceof DBAppNull)  //not sure about this!!!!!
            return false;





        if(operator.equals(">"))
            return compareTo(a,b)>0;

        else if (operator.equals(">="))
            return  compareTo(a,b)>=0;

        else if (operator.equals("<"))
            return compareTo(a,b)<0;

        else if (operator.equals("<="))
            return compareTo(a,b)<=0;

        else if (operator.equals("!="))
            return compareTo(a,b)!=0;

        else if (operator.equals("="))
            return compareTo(a,b)==0;

        else
            throw new DBAppException("ERROR IN COMPARE WITH OPERATOR  METHOD !!");

    }

}
