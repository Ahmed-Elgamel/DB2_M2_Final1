import java.io.Serializable;
import java.util.*;

public class Leaf extends Node implements Serializable {

    Hashtable<Point, ArrayList<Refrence>> pointRefrenceHashtable;

    public Leaf(Point point1, Point point2)
    {
        super(point1,point2);
        pointRefrenceHashtable=new Hashtable<>();

    }

    public Leaf(Object x1 , Object y1 , Object z1 , Object x2 , Object y2 , Object z2)
    {
        super(x1,y1,z1,x2,y2,z2);
        pointRefrenceHashtable=new Hashtable<>();

    }


    public Hashtable<Point, ArrayList<Refrence>> getPointRefrenceHashtable() {
        return pointRefrenceHashtable;
    }

    public void insertReference(Object x , Object y ,Object z,int pageID , int recordID,Object key)
    {
        Point p =new Point(x,y,z);
        if(pointRefrenceHashtable.containsKey(p))
            pointRefrenceHashtable.get(p).add(new Refrence(pageID,recordID,key));
        else {
            ArrayList<Refrence> refrences=new ArrayList<>();
            refrences.add(new Refrence(pageID,recordID,key));
            pointRefrenceHashtable.put(new Point(x, y, z), refrences);
        }

    }

        public void insertReference(Point p,int pageID , int recordID,Object key)
    {
        Point p1 =new Point(p.x,p.y,p.z);
        if(pointRefrenceHashtable.containsKey(p1))
            pointRefrenceHashtable.get(p1).add(new Refrence(pageID,recordID,key));
        else {
            ArrayList<Refrence> refrences=new ArrayList<>();
            refrences.add(new Refrence(pageID,recordID,key));
            pointRefrenceHashtable.put(p1, refrences);
        }
       // System.out.println(pointRefrenceHashtable);

    }


    public void deleteKeys(Point p,Object clusteringKey) throws DBAppException {
      //  Set<Point> keys = getPointRefrenceHashtable().keySet();

        Enumeration<Point> keys=getPointRefrenceHashtable().keys();
        while(keys.hasMoreElements())
        {
            Point key =keys.nextElement();
            Object x =key.x;
            Object y =key.y;
            Object z =key.z;
            boolean ans =true;
            if(!(x instanceof DBAppNull) && !(p.x instanceof DBAppNull))
                ans&=Compare.compareTo(x,p.x)==0;

            if(!(y instanceof DBAppNull) && !(p.y instanceof DBAppNull))
                ans&=Compare.compareTo(y,p.y)==0;

            if(!(z instanceof DBAppNull) && !(p.z instanceof DBAppNull))
                ans&=Compare.compareTo(z,p.z)==0;


                ArrayList<Refrence> references = getPointRefrenceHashtable().get(key);

                for(int i=0;i<getPointRefrenceHashtable().get(key).size();i++)
                {
                    Refrence refrence=references.get(i);
                    if(ans && Compare.compareTo(refrence.key,clusteringKey)==0)
                    {
                        getPointRefrenceHashtable().get(key);
                        references.remove(refrence); //I keep removing from the references of each entry in the leaf
                        break;
                    }

                    if(ans && Compare.compareTo(clusteringKey , getPointRefrenceHashtable().get(key).get(i).getKey())==0)
                        getPointRefrenceHashtable().get(key).remove(i);


                }

            if(getPointRefrenceHashtable().get(key).size()==0)
                getPointRefrenceHashtable().remove(key);




        }
    }

    public void addPointsIfPointsMatchInLeaf(Point p ,ArrayList<Object> points) throws DBAppException {
        Set<Point> keys = getPointRefrenceHashtable().keySet();
        for(Point key :keys)
        {
            Object x =key.x;
            Object y =key.y;
            Object z =key.z;
            boolean ans =true;

            if(!(x instanceof DBAppNull) && !(p.x instanceof DBAppNull))
                ans&=Compare.compareTo(x,p.x)==0;

            if(!(y instanceof DBAppNull) && !(p.y instanceof DBAppNull))
                ans&=Compare.compareTo(y,p.y)==0;

            if(!(z instanceof DBAppNull) && !(p.z instanceof DBAppNull))
                ans&=Compare.compareTo(z,p.z)==0;

            if(ans)
                points.add(new Point(x,y,z));

        }
    }
    public void addRefrencesIfPointsMatchInLeaf(Point p , ArrayList<Refrence> references) throws DBAppException {
        Set<Point> keys = getPointRefrenceHashtable().keySet();

        for(Point key : keys)
        {
            Object x =key.x;
            Object y =key.y;
            Object z =key.z;
            boolean ans =true;
            if(!(x instanceof DBAppNull) && !(p.x instanceof DBAppNull))
                ans&=Compare.compareTo(x,p.x)==0;

            if(!(y instanceof DBAppNull) && !(p.y instanceof DBAppNull))
                ans&=Compare.compareTo(y,p.y)==0;

            if(!(z instanceof DBAppNull) && !(p.z instanceof DBAppNull))
                ans&=Compare.compareTo(z,p.z)==0;


            if(ans)
            {
                ArrayList<Refrence> temp =getPointRefrenceHashtable().get(key);
                for(int i=0;i<temp.size();i++)
                    references.add(new Refrence(temp.get(i).pageId,temp.get(i).recordId,temp.get(i).key));
            }
        }
    }

    public void addRefrencesIfPointsMatchInLeafWithOperators(Point p  , String [] operators , ArrayList<Refrence>references,int type) throws DBAppException {
        Set<Point> keys = getPointRefrenceHashtable().keySet();

        for(Point key : keys)
        {
            Object x =key.x;
            Object y =key.y;
            Object z =key.z;
            //System.out.println(Arrays.deepToString(operators));
            boolean ans =true;
            if(type==1) //all anded together
            {
                if(!(x instanceof DBAppNull) && !(p.x instanceof DBAppNull)) //operator impossible to be null  if x and y are not null!!
                    ans&=Compare.compareToWithOperator(x,p.x,operators[0]);

                if(!(y instanceof DBAppNull) && !(p.y instanceof DBAppNull))
                    ans&=Compare.compareToWithOperator(y,p.y,operators[1]);

                if(!(z instanceof DBAppNull) && !(p.z instanceof DBAppNull))
                    ans&=Compare.compareToWithOperator(z,p.z,operators[2]);

//                if(  (!(x instanceof Null) && (p.x instanceof Null)) || ((x instanceof Null) && !(p.x instanceof Null)))
//                    ans=false;
//
//                if( (!(y instanceof Null) && (p.y instanceof Null)) || ((y instanceof Null) && !(p.y instanceof Null)) )
//                    ans=false;
//
//                if( (!(z instanceof Null) && (p.z instanceof Null)) || ((z instanceof Null) && !(p.z instanceof Null)))
//                    ans=false;

//                System.out.println(x+" "+operators[0]+" "+p.x+ " , "+
//                                   y+" "+operators[1]+" "+p.y+ " , "+
//                                   z+" "+operators[2]+" "+p.z);

                if(ans)
                {
                    ArrayList<Refrence> temp =getPointRefrenceHashtable().get(key);
                    for(int i=0;i<temp.size();i++)
                        references.add(new Refrence(temp.get(i).pageId,temp.get(i).recordId,temp.get(i).key));
                }
            }
            else if (type==2)// all ored together
            {
                ans=false; //not sure about this
                if(!(x instanceof DBAppNull) && !(p.x instanceof DBAppNull)) //operator impossible to be null  if x and y are not null!!
                    ans|=Compare.compareToWithOperator(x,p.x,operators[0]);

                if(!(y instanceof DBAppNull) && !(p.y instanceof DBAppNull))
                    ans|=Compare.compareToWithOperator(y,p.y,operators[1]);

                if(!(z instanceof DBAppNull) && !(p.z instanceof DBAppNull))
                    ans|=Compare.compareToWithOperator(z,p.z,operators[2]);

                if( x instanceof DBAppNull && p.x instanceof DBAppNull)
                    ans=true;

                if( y instanceof DBAppNull && p.y instanceof DBAppNull)
                    ans=true;

                if( z instanceof DBAppNull && p.z instanceof DBAppNull)
                    ans=true;


                if(ans)
                {
                    ArrayList<Refrence> temp =getPointRefrenceHashtable().get(key);
                    for(int i=0;i<temp.size();i++)
                        references.add(new Refrence(temp.get(i).pageId,temp.get(i).recordId,temp.get(i).key));
                }
            }
        }
    }


    public void decrementPageIds(int pageId)
    {
        Enumeration<Point> keys= getPointRefrenceHashtable().keys();

        while(keys.hasMoreElements())
        {
            Point key =keys.nextElement();
            ArrayList<Refrence> refrences=getPointRefrenceHashtable().get(key);

            for(int i=0;i<refrences.size();i++)
            {
                if(refrences.get(i).pageId>=pageId)
                    refrences.get(i).pageId--;
            }

        }
    }



    public int  getSize()
    {
        return pointRefrenceHashtable.size();
    }



    public String toString()
    {
        Set<Point> keys =pointRefrenceHashtable.keySet();
        StringBuilder sb =new StringBuilder("");
        sb.append("(LEAF)");
        sb.append("RANGE-> MIN: "+getMinPoint().toString()+" MAX: "+getMaxPoint().toString());
        sb.append(" HASHTABLE PAIRS :");
        int i=0;
        for(Point key : keys)
        {
            sb.append("POINT: "+"X= "+key.x+" Y= "+key.y+" Z= "+key.z+"  REFERENCES: ");
            sb.append(pointRefrenceHashtable.get(key).toString());
            if(i!=pointRefrenceHashtable.size()-1)
                sb.append(" , ");
            i++;
        }
        return sb.toString();
    }


}
