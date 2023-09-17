public class testOctree {

    public static void main(String[] args) throws DBAppException {
        Integer x1=0;
        Integer y1=0;
        Integer z1=0;

        Integer x2=2000;
        Integer y2=2000;
        Integer z2=2000;

        Octree octree = new Octree(x1,y1,z1,x2,y2,z2);
        Point p1=new Point(1,10,100);
        Point p2=new Point(1500,1590,90);
        //Point p2=new Point(2,20,200);
        Point p3=new Point(3,30,300);
        Point p4=new Point(4,40,400);
        Point p5=new Point(5,50,500);

        octree.insert(p1,1,1,(1));
        octree.insert(p1,1,2,(2));
        octree.insert(p1,1,3,(3));
        octree.insert(p1,1,90,(4));
        //System.out.println("path "+octree.path);
        octree.accumulator.clear();

        octree.insert(p2,2,2,22);
        //System.out.println("path "+octree.path);
        octree.accumulator.clear();
        octree.insert(p3,3,3,33);
        octree.insert(p4,4,0,44);
        octree.accumulator.clear();
        octree.insert(p5,5,0,55);
        //System.out.println("path "+octree.path);
        octree.insert(p1,1,1,5);


        System.out.println(octree.search(new Point(1,10,100)));
        System.out.println(octree.search(p5));
        System.out.println("--------------------------------------------------------------------------------------");
        octree.printOctree();
        System.out.println("--------------------------------------------------------------------------------------------");
        octree.delete(new Point(1,new DBAppNull(null),new DBAppNull(null)),5);
        octree.printOctree();
        System.out.println(octree.search(new Point(new DBAppNull(null),new DBAppNull(null),new DBAppNull(null))));



//
//        Hashtable<Point,Integer> ht1=new Hashtable<>();
//        ht1.put(p1,1);
//        ht1.put(p2,1);
//        System.out.println(ht1.containsKey(new Point(1,10,100)));
//        System.out.println(p1.equals(new Point(1,10,100)));
//        System.out.println(ht1.get(new Point(1,10,100)));
//
//        ht1.remove(new Point(1,10,100));
//        System.out.println(ht1.get(new Point(1,10,100)));




    }
}
