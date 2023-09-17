import java.io.Serializable;

public class NonLeaf extends Node implements Serializable {


    Node[] children ;


    public NonLeaf(Point point1, Point point2)
    {
        super(point1,point2);
    }

    public NonLeaf(Object x1 , Object y1 , Object z1 , Object x2 , Object y2 , Object z2)
    {
        super(x1,y1,z1,x2,y2,z2);
    }

    public void initializeChildren() throws DBAppException {


        Object minX=getMinX();
        Object maxX=getMaxX();

        Object minY=getMinY();
        Object maxY=getMaxY();

        Object minZ=getMinZ();
        Object maxZ=getMaxZ();

        Object midX = Point.getMid(endPoints[0].x, endPoints[1].x);
        Object midY= Point.getMid(endPoints[0].y, endPoints[1].y);
        Object midZ= Point.getMid(endPoints[0].z, endPoints[1].z);
        //System.out.println(midX+" "+midY+" "+midZ+" "+minX+" "+minY+" "+minZ+" "+maxX+" "+maxY+" "+maxZ+" ");


        children=new Node[8];

        Point p1=new Point(minX,minY,minZ);
        Point p2=new Point(midX,midY,midZ);
        children[0]=new Leaf(p1,p2);

        Point p3=new Point(minX,minY, Point.increment(midZ));
        Point p4=new Point(midX,midY,maxZ);
        children[1]=new Leaf(p3,p4);

        Point p5=new Point(minX, Point.increment(midY),minZ);
        Point p6=new Point(midX,maxY,midZ);
        children[2]=new Leaf(p5,p6);


        Point p7=new Point(minX, Point.increment(midY), Point.increment(midZ));
        Point p8=new Point(midX,maxY,maxZ);
        children[3]=new Leaf(p7,p8);

        Point p9=new Point(Point.increment(midX),minY,minZ);
        Point p10=new Point(maxX,midY,midZ);
        children[4]=new Leaf(p9,p10);

        Point p11=new Point(Point.increment(midX),minY, Point.increment(midZ));
        Point p12=new Point(maxX,midY,maxZ);
        children[5]=new Leaf(p11,p12);

        Point p13=new Point(Point.increment(midX), Point.increment(midY),minZ);
        Point p14=new Point(maxX,maxY,midZ);
        children[6]=new Leaf(p13,p14);


        Point p15=new Point(Point.increment(midX), Point.increment(midY), Point.increment(midZ));
        Point p16=new Point(maxX,maxY,maxZ);
        children[7]=new Leaf(p15,p16);

        for (int i=0;i<8;i++)
            children[i].parent=this;



    }
    public String toString()
    {
        return "(NON LEAF) RANGE-> "+"MIN: "+getMinPoint().toString()+" , MAX: "+getMaxPoint().toString();
    }
}
