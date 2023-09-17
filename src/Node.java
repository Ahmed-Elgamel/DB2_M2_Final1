import java.io.Serializable;

public class Node implements Serializable {
    Point [] endPoints;
    Node parent;

    public Node(){

    };
    public Node(Point point1, Point point2)
    {
        endPoints=new Point[2];
        endPoints[0]=point1;
        endPoints[1]=point2;
    }

    public Node(Object x1 , Object y1 , Object z1 , Object x2 , Object y2 , Object z2)
    {
        endPoints=new Point[2];
        endPoints[0]=new Point(x1,y1,z1);
        endPoints[1]=new Point(x2,y2,z2);
    }
    public Point getMaxPoint()
    {
        return endPoints[1];
    }
    public Point getMinPoint()
    {
        return endPoints[0];
    }
    public Object getMaxX()
    {
        return endPoints[1].x;
    }
    public Object getMinX()
    {
        return endPoints[0].x;
    }


    public Object getMaxY()
    {
        return endPoints[1].y;
    }
    public Object getMinY()
    {
        return endPoints[0].y;
    }


    public Object getMaxZ()
    {
        return endPoints[1].z;
    }
    public Object getMinZ()
    {
        return endPoints[0].z;
    }



    public  Object getMidX() throws DBAppException {
        Object x1 = endPoints[0].x;
        Object x2 = endPoints[1].x;

        return Point.getMid(x1,x2);
    }

    public  Object getMidY() throws DBAppException {
        Object y1 = endPoints[0].y;
        Object y2 = endPoints[1].y;

        return Point.getMid(y1,y2);
    }
    public  Object getMidZ() throws DBAppException {
        Object z1 = endPoints[0].z;
        Object z2 = endPoints[1].z;

        return Point.getMid(z1,z2);
    }








    public boolean pointCanBeInsideMe(Point p) throws DBAppException {
        Object x =p.x;
        Object y =p.y;
        Object z =p.z;
        boolean ans =true;

        if(x instanceof DBAppNull && y instanceof DBAppNull &&z instanceof DBAppNull)
            return true; //not sure

        if(!(x instanceof DBAppNull))
            ans&= Compare.compareTo(getMinX(),x)<=0 && Compare.compareTo(getMaxX(),x)>=0;

        if(!(y instanceof DBAppNull))
            ans&= Compare.compareTo(getMinY(),y)<=0 && Compare.compareTo(getMaxY(),y)>=0;

        if(!(z instanceof DBAppNull))
            ans&= Compare.compareTo(getMinZ(),z)<=0 && Compare.compareTo(getMaxZ(),z)>=0;

        return ans;
    }


    public boolean pointCanBeInsideMe(Point p,String []operators,int type) throws DBAppException {
        Object x =p.x;
        Object y =p.y;
        Object z =p.z;
        boolean ans =true;

        if(x instanceof DBAppNull && y instanceof DBAppNull &&z instanceof DBAppNull)
            return true; //not sure



        if(type==1)
        {
            if(operators[0]!=null && !(x instanceof DBAppNull) && !operators[0].equals("!=")) {

                if(operators[0].equals("="))
                    ans&= Compare.compareTo(getMinX(),x)<=0 && Compare.compareTo(getMaxX(),x)>=0;

                else if (operators[0].equals(">") || operators[0].equals(">="))
                    ans&=Compare.compareToWithOperator(getMaxX(),x,operators[0]);

                else if (operators[0].equals("<") || operators[0].equals("<="))
                    ans&=Compare.compareToWithOperator(getMinX(),x,operators[0]);

            }

            if(operators[1]!=null &&!(y instanceof DBAppNull) && !operators[1].equals("!=")) {

                if (operators[1].equals("="))
                    ans &= Compare.compareTo(getMinY(), y) <= 0 && Compare.compareTo(getMaxY(), y) >= 0;

                else if (operators[1].equals(">") || operators[1].equals(">="))
                    ans &= Compare.compareToWithOperator(getMaxY(), y, operators[1]);

                else if (operators[1].equals("<") || operators[1].equals("<="))
                    ans &= Compare.compareToWithOperator(getMinY(), y, operators[1]);
            }


            if (operators[2]!=null &&!(z instanceof DBAppNull) && !operators[2].equals("!=")) {


                if (operators[2].equals("="))
                    ans &= Compare.compareTo(getMinZ(), z) <= 0 && Compare.compareTo(getMaxZ(), z) >= 0;

                else if (operators[2].equals(">") || operators[2].equals(">="))
                    ans &= Compare.compareToWithOperator(getMaxZ(), z, operators[2]);

                else if (operators[2].equals("<") || operators[2].equals("<="))
                    ans &= Compare.compareToWithOperator(getMinZ(), z, operators[2]);

            }

            return ans;
        }
        else //type 2
        {
            ans=false; //not sure about this!!
            if(x instanceof DBAppNull || y instanceof DBAppNull || z instanceof DBAppNull)
                ans=true;

            if(operators[0]!=null &&!(x instanceof DBAppNull) && !operators[0].equals("!=")) {

                if(operators[0].equals("="))
                {
                    ans|= Compare.compareTo(getMinX(),x)<=0 && Compare.compareTo(getMaxX(),x)>=0;

                }

                else if (operators[0].equals(">") || operators[0].equals(">="))
                {
                    ans|=Compare.compareToWithOperator(getMaxX(),x,operators[0]);
                }

                else if (operators[0].equals("<") || operators[0].equals("<="))
                {
                    ans|=Compare.compareToWithOperator(getMinX(),x,operators[0]);

                }

            }

            if(operators[1]!=null &&!(y instanceof DBAppNull) && !operators[1].equals("!=")) {

                if (operators[1].equals("="))
                {
                    ans |= Compare.compareTo(getMinY(), y) <= 0 && Compare.compareTo(getMaxY(), y) >= 0;

                }

                else if (operators[1].equals(">") || operators[1].equals(">="))
                {
                    ans |= Compare.compareToWithOperator(getMaxY(), y, operators[1]);

                }

                else if (operators[1].equals("<") || operators[1].equals("<="))
                {
                    ans |= Compare.compareToWithOperator(getMinY(), y, operators[1]);

                }
            }


            if (operators[2]!=null &&!(z instanceof DBAppNull) && !operators[2].equals("!=")) {


                if (operators[2].equals("="))
                {
                    ans |= Compare.compareTo(getMinZ(), z) <= 0 && Compare.compareTo(getMaxZ(), z) >= 0;

                }

                else if (operators[2].equals(">") || operators[2].equals(">="))
                {
                    ans |= Compare.compareToWithOperator(getMaxZ(), z, operators[2]);

                }

                else if (operators[2].equals("<") || operators[2].equals("<="))
                {
                    ans |= Compare.compareToWithOperator(getMinZ(), z, operators[2]);

                }

            }

            return ans;
        }
    }






}
