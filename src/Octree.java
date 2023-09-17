import java.io.*;
import java.util.*;

public class Octree implements Serializable {

    private String name;
    private String xDimensionName;
    private String yDimensionName;

    private String zDimensionName;

    private Node root;
    private static int maxSize;

    ArrayList<Object> accumulator =new ArrayList<>();

    private static ArrayList<Refrence> searchOutput;

    public String getName() {
        return name;
    }

    public String gexDimensionName() {
        return xDimensionName;
    }

    public String getyDimensionName() {
        return yDimensionName;
    }

    public String getzDimensionName() {
        return zDimensionName;
    }


    //normal constructor
    public Octree(String tableName,String name, String column1, String column2, String column3) throws IOException, DBAppException {
        //traverse the whole table and insert it into the Octree

        Point[] endpoints=Point.getEndPoints(tableName,column1,column2,column3);
        root=new Leaf(endpoints[0].x,endpoints[0].y,endpoints[0].z ,endpoints[1].x,endpoints[1].y,endpoints[1].z );
        root.parent=root;

        String leafMaxEntries =(String) CsvReader.readAndLoadConfigFile().get("MaximumEntriesinOctreeNode");
        maxSize = Integer.parseInt(leafMaxEntries);
        this.name=name;
        this.xDimensionName=column1;
        this.yDimensionName=column2;
        this.zDimensionName=column3;
        Table table =Table.deserliazeTable(tableName);

        //loop over the whole table and insert values
        for(int i=0;i<table.paths.size();i++)
        {
            Page page =Page.deserializePage(tableName,i);
            for(int j=0;j<page.allRecords.size();j++)
            {
                Tuple tuple =page.allRecords.get(j);
                Hashtable<String ,Object> record = tuple.getRecord();
                Object xDimensionValue=record.get(xDimensionName);
                Object yDimensionValue=record.get(yDimensionName);
                Object zDimensionValue=record.get(zDimensionName);
                Object key =record.get(table.clusteringColumnName);

                insert(xDimensionValue,yDimensionValue,zDimensionValue,i,-1,key);

            }
        }
        table.serializeTable();

    }

    //testing constructor
    public Octree(Object x1 , Object y1 , Object z1  , Object x2 , Object y2 , Object z2)
    {
       root=new Leaf(x1,y1,z1,x2,y2,z2);
       root.parent=root;
       maxSize=1;
        //System.out.println("root "+root.toString());
    }

    public void insert(Object x , Object y ,Object z,int pageId , int recordId,Object key) throws DBAppException {
        Point p =new Point(x,y,z);
        insert(root,p,pageId,recordId,key);
        //insert2(root,p,pageId,recordId,key);

    }
    public void insert(Point p , int pageId , int recordId,Object key) throws DBAppException {
        Point p2 =new Point(p.x,p.y,p.z);
        //accumulator.add("ROOT "+root);
       // System.out.println("root "+root);
        insert(root,p2,pageId,recordId,key);
        //insert2(root,p2,pageId,recordId,key);
    }


    private void insert(Node node , Point p , int pageId , int recordId,Object key) throws DBAppException {
//        if(outOfBounds(node,p))
//            throw new DBAppException("POINT "+p.toString()+ node.toString()+" IS OUT OF BOUNDS FOR THE OCTREE DIMENSIONS RANGES!!");

        if(node instanceof Leaf)
        {
            Leaf leaf =(Leaf) node;

            if(leaf.getSize()<maxSize) {
                {
                    //accumulator.add("leaf has place in hashtable (base case 1)");
                    leaf.insertReference(p,pageId,recordId,key);
                }
            }
            else
            {
                if(p.x.equals(2))
                    System.out.println("size is "+leaf.getSize()+" i made a split");
                //accumulator.add("splitting must happen here(base case 2)");
                leaf.insertReference(p,pageId,recordId,key);
                split(node);
            }

            return;
        }

        int branchNum = getBranchNum(node, p);
        NonLeaf nonLeaf = (NonLeaf) node;


        Node child = nonLeaf.children[branchNum];

        //accumulator.add(child);
        insert(child, p ,pageId , recordId,key);

    }


//    private void insert2(Node node , Point p , int pageId , int recordId,Object key) throws DBAppException {
//        if(outOfBounds(node,p))
//            throw new DBAppException("POINT "+p.toString()+ node.toString()+" IS OUT OF BOUNDS FOR THE OCTREE DIMENSIONS RANGES!!");
//
//        if(node instanceof Leaf)
//        {
//            Leaf leaf =(Leaf) node;
//            if(leaf.getSize()<maxSize)
//                    leaf.insertReference(p,pageId,recordId,key);
//
//            else
//            {
//                leaf.insertReference(p,pageId,recordId,key);
//                split(node);
//            }
//
//            return;
//        }
//
//        NonLeaf nonLeaf = (NonLeaf) node;
//        for(int i=0;i<nonLeaf.children.length;i++)
//        {
//            Node child = nonLeaf.children[i];
//            if(child.pointCanBeInsideMe(p))
//                insert2(child,p,pageId,recordId,key);
//        }
//
//
//    }
//
//



    public ArrayList<Refrence> search(Object x , Object y , Object z) throws DBAppException {
        searchOutput=new ArrayList<>();
//        return search(root, new Point(x,y,z));
         search2(root, new Point(x,y,z));
         return searchOutput;

    }
    public ArrayList<Refrence> search(Point p) throws DBAppException {
        searchOutput=new ArrayList<>();
        //return search(root,p);
        search2(root,p);
        return searchOutput;
    }

    private ArrayList<Refrence> search(Node node , Point p) throws DBAppException {
        if(outOfBounds(node,p))
            throw new DBAppException("POINT IS OUT OF BOUNDS FOR THE OCTREE DIMENSIONS RANGES!!");

        if(node instanceof Leaf)
        {
          Leaf leaf = (Leaf) node;
          return leaf.getPointRefrenceHashtable().get(p)==null ? (new ArrayList<>()): (leaf.getPointRefrenceHashtable().get(p));
        }

        int branchNum = getBranchNum(node, p);
        NonLeaf nonLeaf = (NonLeaf) node;

        Node child = nonLeaf.children[branchNum];

        ArrayList<Refrence> ans = search(child, p);
        return ans;


    }

    private void search2(Node node , Point p) throws DBAppException {
        if(outOfBounds(node,p))
            throw new DBAppException("POINT IS OUT OF BOUNDS FOR THE OCTREE DIMENSIONS RANGES!!");

        if(node instanceof Leaf)
        {
            Leaf leaf = (Leaf) node;
            leaf.addRefrencesIfPointsMatchInLeaf(p,searchOutput);
            return;
        }

        NonLeaf nonLeaf = (NonLeaf) node;
        for(int i=0;i<nonLeaf.children.length;i++)
        {
            Node child = nonLeaf.children[i];
            if(child.pointCanBeInsideMe(p))
                search2(child,p);
        }



    }







    public void delete(Object x ,Object y ,Object z,Object key) throws DBAppException {
        delete(root, new Point(x,y,z),key);
    }
    public void delete(Point p,Object key) throws DBAppException {
        delete(root,p,key);
    }
    private void delete(Node node , Point p,Object key) throws DBAppException {

        if(outOfBounds(node,p))
            throw new DBAppException("POINT IS OUT OF BOUNDS FOR THE OCTREE DIMENSIONS RANGES!!");

        if(node instanceof Leaf)
        {
            Leaf leaf = (Leaf) node;
            leaf.deleteKeys(p,key);
            return;
        }

        NonLeaf nonLeaf = (NonLeaf) node;

        for(int i=0;i<nonLeaf.children.length;i++)
        {
            Node child = nonLeaf.children[i];
            if(child.pointCanBeInsideMe(p))
                delete(child,p,key);
        }


    }

    public ArrayList<Refrence> selectFromTree(ArrayList<String>columnNames , ArrayList<Object> columnValues , ArrayList<String>operators,int type) throws DBAppException {
        Object valueX=new DBAppNull(null);
        Object valueY=new DBAppNull(null);
        Object valueZ=new DBAppNull(null);
        String operatorX = null;
        String operatorY=null;
        String operatorZ=null;

        String [] operatorsOrdered=new String[3];
        Arrays.fill(operatorsOrdered,null);

       if(columnValues.size()>0)
       {
           if(columnNames.get(0).equals(gexDimensionName()))
           {
               valueX=columnValues.get(0);
               operatorX=operators.get(0);
               operatorsOrdered[0]=operatorX;
           }
           else if(columnNames.get(0).equals(getyDimensionName()))
           {
               valueY=columnValues.get(0);
               operatorY=operators.get(0);
               operatorsOrdered[0]=operatorY;
           }
           else if(columnNames.get(0).equals(getzDimensionName()))
           {
               valueZ=columnValues.get(0);
               operatorZ=operators.get(0);
               operatorsOrdered[0]=operatorZ;
           }

       }

       if(columnValues.size()>1)
       {
           if(columnNames.get(1).equals(gexDimensionName()))
           {
               valueX=columnValues.get(1);
               operatorX=operators.get(1);
               operatorsOrdered[1]=operatorX;
           }
           else if(columnNames.get(1).equals(getyDimensionName()))
           {
               valueY=columnValues.get(1);
               operatorY=operators.get(1);
               operatorsOrdered[1]=operatorY;
           }
           else if(columnNames.get(1).equals(getzDimensionName()))
           {
               valueZ=columnValues.get(1);
               operatorZ=operators.get(1);
               operatorsOrdered[1]=operatorZ;
           }
       }


       if(columnValues.size()==3)
       {
           if(columnNames.get(2).equals(gexDimensionName()))
           {
               valueX=columnValues.get(2);
               operatorX=operators.get(2);
               operatorsOrdered[2]=operatorX;
           }
           else if(columnNames.get(2).equals(getyDimensionName()))
           {
               valueY=columnValues.get(2);
               operatorY=operators.get(2);
               operatorsOrdered[2]=operatorY;
           }
           else if(columnNames.get(2).equals(getzDimensionName()))
           {
               valueZ=columnValues.get(2);
               operatorZ=operators.get(2);
               operatorsOrdered[2]=operatorZ;
           }
       }

       searchOutput=new ArrayList<>();
       accumulator=new ArrayList<>();
        operatorsOrdered[0]=operatorX;
        operatorsOrdered[1]=operatorY;
        operatorsOrdered[2]=operatorZ;

       searchWithOperators(root,new Point(valueX,valueY,valueZ),operatorsOrdered,type);

       System.out.println("INDEX "+name+" is used on columms "+gexDimensionName()+ " "+operatorsOrdered[0]+" " +valueX+" , "+
                                                               getyDimensionName()+ " "+operatorsOrdered[1]+" " +valueY+" , "+
                                                               getzDimensionName()+ " "+operatorsOrdered[2]+" " +valueZ);


//        System.out.println("OUTPUT FROM SELECT ON OCTREE INDEX IS "+searchOutput);
//        System.out.println("PATH OF THE SELECT ON OCTREE IS  "+accumulator);

        return searchOutput;


    }
    private void searchWithOperators(Node node , Point p,String[]operators,int type) throws DBAppException {

        if(node instanceof Leaf)
        {
            Leaf leaf = (Leaf) node;
            accumulator.add(leaf);
            leaf.addRefrencesIfPointsMatchInLeafWithOperators(p,operators,searchOutput,type);
            return;
        }

        NonLeaf nonLeaf = (NonLeaf) node;
        accumulator.add(nonLeaf);
        for(int i=0;i<nonLeaf.children.length;i++)
        {
            Node child = nonLeaf.children[i];
            if(child.pointCanBeInsideMe(p,operators,type))
            {

                searchWithOperators(child,p,operators,type);
            }
        }



    }



    public static void updateAllOctrees(String tableName,Object key , Hashtable<String,Object> oldRecord ,Hashtable<String,Object> newRecord,int pageId) throws IOException, DBAppException {
        Map<String,ArrayList<String >> allOctrees = CsvReader.getAllOctrees(tableName);

        for(String octreeName : allOctrees.keySet())
        {
            Octree octree = Octree.deserliazeOctree(octreeName);
            String xDimension=octree.gexDimensionName();
            String yDimension=octree.getyDimensionName();
            String zDimension=octree.getzDimensionName();

            Object oldX =oldRecord.get(xDimension); //==null ? new Null(null) : newRecord.get(xDimension);
            Object oldY =oldRecord.get(yDimension); //==null ? new Null(null) : newRecord.get(yDimension);
            Object oldZ =oldRecord.get(zDimension); //==null ? new Null(null) : newRecord.get(zDimension);

            ArrayList<Object> pointsOfInterest =octree.getPoints(oldX,oldY,oldZ,key); //all points that satisfy x y z

            if(pointsOfInterest.size()==0)
                System.out.println("WHATTT");

            for(int i=0;i<pointsOfInterest.size();i++)
            {
                Point p = (Point) pointsOfInterest.get(i);
                Object updatedX= newRecord.get(xDimension)==null ? (oldX) : (newRecord.get(xDimension));
                Object updatedY=newRecord.get(yDimension)==null ? (oldY) : (newRecord.get(yDimension));
                Object updatedZ=newRecord.get(zDimension)==null ? (oldZ) : (newRecord.get(zDimension));

                octree.delete(oldX,oldY,oldZ,key);
                octree.insert(updatedX,updatedY,updatedZ,pageId,-1,key);
            }


            octree.serializeOctree();

        }

    }

    public ArrayList<Object> getPoints (Object x , Object y ,Object z,Object key) throws DBAppException {
        accumulator=new ArrayList<>();
        getPoints(root,new Point(x,y,z) , key );
        return accumulator;
    }

    public void getPoints(Node node, Point p,Object key) throws DBAppException {
        if(outOfBounds(node,p))
            throw new DBAppException("POINT IS OUT OF BOUNDS FOR THE OCTREE DIMENSIONS RANGES!!");

        if(node instanceof Leaf)
        {
            Leaf leaf = (Leaf) node;
            Set<Point> points = leaf.getPointRefrenceHashtable().keySet();
            for(Point point :points)
            {
                ArrayList<Refrence> refrences =leaf.getPointRefrenceHashtable().get(point);
                for(int i=0;i<refrences.size();i++)
                {
                    if(Compare.compareTo(refrences.get(i).key, key)==0)
                        accumulator.add(point);
                }
            }

            return;
        }

        NonLeaf nonLeaf = (NonLeaf) node;
        for(int i=0;i<nonLeaf.children.length;i++)
        {
            Node child = nonLeaf.children[i];
            if(child.pointCanBeInsideMe(p))
                getPoints(child,p,key);
        }



    }


    public static void deleteFromAllOctrees(String tableName , Tuple t ,Object key) throws IOException, DBAppException {
        Map<String,ArrayList<String >> allOctrees = CsvReader.getAllOctrees(tableName);

        for(String octreeName : allOctrees.keySet())
        {
            Octree octree = Octree.deserliazeOctree(octreeName);
            Object x =t.record.get(octree.gexDimensionName());
            Object y =t.record.get(octree.getyDimensionName());
            Object z =t.record.get(octree.getzDimensionName());
            octree.delete(x,y,z,key);
            octree.serializeOctree();
        }


    }

    public static void decrementPageIdsInAllOctrees(String tableName,int pageId) throws IOException, DBAppException {
        Map<String,ArrayList<String >> allOctrees = CsvReader.getAllOctrees(tableName);

        for(String octreeName : allOctrees.keySet())
        {
            Octree octree = Octree.deserliazeOctree(octreeName);
            octree.accumulator=new ArrayList<>();
            octree.getAllLeafs(octree.root); //adds leafs to the accumulator

            for(int i=0;i<octree.accumulator.size();i++)
            {
                Leaf leaf=(Leaf) octree.accumulator.get(i);
                leaf.decrementPageIds(pageId);
            }
            octree.serializeOctree();
        }

    }

    public  void getAllLeafs(Node node)
    {
        if(node instanceof Leaf)
        {
            Leaf leaf= (Leaf)node;
            accumulator.add(leaf);
            return;
        }
        NonLeaf nonLeaf =(NonLeaf) node;
        for(int i=0;i<nonLeaf.children.length;i++)
            getAllLeafs(nonLeaf.children[i]);
    }




    public  int getBranchNum(Node node , Point point) throws DBAppException {


        Object rangeX =Point.increment(Point.getMid(node.getMinX(), node.getMaxX()));
        Object rangeY=Point.increment(Point.getMid(node.getMinY(), node.getMaxY()));
        Object rangeZ=Point.increment(Point.getMid(node.getMinZ(), node.getMaxZ()));

        int x=0;

        if(!(point.x instanceof DBAppNull))
        {
            if(Compare.compareTo(point.x,rangeX)>=0)
                x=+4;
        }
        if(!(point.y instanceof DBAppNull))
        {
            if(Compare.compareTo(point.y,rangeY)>=0)
                x+=2;
        }
        if(!(point.z instanceof DBAppNull))
        {
            if(Compare.compareTo(point.z,rangeZ)>=0)
                x+=1;
        }
        return x;



//        if(Compare.compareTo(point.x,rangeX)<0   && Compare.compareTo(point.y,rangeY)<0    && Compare.compareTo(point.z,rangeZ)<0)
//            x= 0;
//
//        else if(Compare.compareTo(point.x,rangeX)<0   && Compare.compareTo(point.y,rangeY)<0    && Compare.compareTo(point.z,rangeZ)>=0)
//            x= 1;
//
//        else if(Compare.compareTo(point.x,rangeX)<0   && Compare.compareTo(point.y,rangeY)>=0   && Compare.compareTo(point.z,rangeZ)<0)
//            x= 2;
//
//        else if(Compare.compareTo(point.x,rangeX)<0   && Compare.compareTo(point.y,rangeY)>=0   && Compare.compareTo(point.z,rangeZ)>=0)
//            x= 3;
//
//        else if(Compare.compareTo(point.x,rangeX)>=0  && Compare.compareTo(point.y,rangeY)<0    && Compare.compareTo(point.z,rangeZ)<0)
//            x= 4;
//
//        else if(Compare.compareTo(point.x,rangeX)>=0  && Compare.compareTo(point.y,rangeY)<0    && Compare.compareTo(point.z,rangeZ)>=0)
//            x= 5;
//
//        else if(Compare.compareTo(point.x,rangeX)>=0  && Compare.compareTo(point.y,rangeY)>=0   && Compare.compareTo(point.z,rangeZ)<0)
//            x= 6;
//
//        else if(Compare.compareTo(point.x,rangeX)>=0  && Compare.compareTo(point.y,rangeY)>=0   && Compare.compareTo(point.z,rangeZ)>=0)
//            x= 7;
//        else
//            throw new DBAppException("ERROR IN GET BRANCH NUM METHOD !! ");


//        return x;

    }




    public boolean outOfBounds(Node node , Point point) throws DBAppException {
        Object minX=node.getMinX();
        Object miny=node.getMinY();
        Object minZ=node.getMinZ();

        Object maxX=node.getMaxX();
        Object maxY=node.getMaxY();
        Object maxZ=node.getMaxZ();
        boolean ans =false;

        if(!(point.x instanceof DBAppNull))
            ans|=(Compare.compareTo(point.x,minX)<0 || Compare.compareTo(point.x,maxX)>0);
        if(!(point.y instanceof DBAppNull))
            ans|=(Compare.compareTo(point.y,miny)<0 || Compare.compareTo(point.y,maxY)>0);
        if(!(point.z instanceof DBAppNull))
            ans|=(Compare.compareTo(point.z,minZ)<0 || Compare.compareTo(point.z,maxZ)>0);

        return ans;

    }







    public void split(Node node) throws DBAppException {
        if(node instanceof NonLeaf)
            throw new DBAppException("CAN NOT SPLIT A NON LEAF NODE");

        Leaf leaf =(Leaf)node;

        //get the end Points of the cube
        Point minPoint=node.getMinPoint();
        Point maxPoint=node.getMaxPoint();

        //initialize new non leaf node
        NonLeaf newNode=new NonLeaf(minPoint,maxPoint);
        //initialize new non leaf node children ranges
        newNode.initializeChildren();


        Set<Point> keys = leaf.getPointRefrenceHashtable().keySet();

        //loop on the PointReferenceHashtable in the original leaf and distribute them on children
        for(Point key : keys)
        {
            int childNum = getBranchNum(newNode , key); //for each key in the original leaf node find its new place in the new children
            Leaf child= (Leaf) newNode.children[childNum];

            ArrayList<Refrence> references =leaf.getPointRefrenceHashtable().get(key);

            for(int i=0;i<references.size();i++)
            {
                child.insertReference(key,references.get(i).pageId,references.get(i).recordId,references.get(i).key);
            }

        }
        //the newNode is now an internal node with 8 children with their corresponding values
        if(node.parent!=null)
        {
            int branchNum=getBranchNum(node.parent,new Point(node.getMinX(),node.getMinY(),node.getMinZ()));

            if(node.parent instanceof Leaf)//only the tree is one leaf
            {
                root=newNode;
                root.parent=root;
                node=null;

                for(int i=0;i<8;i++)
                    newNode.children[i].parent=root;

            }
            else {//parent is a non leaf
                NonLeaf parent=(NonLeaf) node.parent;
                parent.children[branchNum]=newNode;
                newNode.parent=node.parent;
                node=null;

            }


        }

        else
            throw new DBAppException("THE NODE HAS NO PARENT !!");


    }

    public void serializeOctree()
    {
        try {

            FileOutputStream fileOut = new FileOutputStream("Indices"+ File.separator+name+".ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
        }
        catch (Exception e)
        {
            System.out.println("ERROR IN SERIALIZE OCTREE METHOD IN CLASS OCTREE FOR OCTREE NAME "+name);
            e.printStackTrace();
        }

    }

    public static Octree deserliazeOctree(String octreeName)
    {
        Octree octree=null;

        try {
            FileInputStream fileIn = new FileInputStream("Indices"+File.separator+octreeName+".ser");
            ObjectInputStream in =new ObjectInputStream(fileIn);
            octree=(Octree) in.readObject();
            in.close();
            fileIn.close();
        }
        catch(Exception e)
        {
            System.out.println("ERROR IN DESERIALIZE OCTREE METHOD IN CLASS OCTREE FOR OCTREE NAME "+octreeName);
        }
        return octree;

    }





    public  void printOctree() {
        //printOctree(root, "", 0);
        //printOctree(root,0);
        printOctree(root,"",true);
    }

    private static void printOctree(Node node, String prefix, int depth) {
        if (node == null) {
            return;
        }

        for (int i = 0; i < depth; i++) {
            System.out.print("  ");
        }

        if (node instanceof Leaf) {
            Leaf leaf = (Leaf)node;
            System.out.println(prefix + "- Leaf "+leaf.toString());
        } else {
            NonLeaf nonleaf = (NonLeaf) node;
            System.out.println(prefix + "- Node");
            for (int i = 0; i < 8; i++) {
                printOctree(nonleaf.children[i], prefix + "  ", depth + 1);
            }
        }
    }
    private void printOctree(Node node, int level) {
        if (node instanceof Leaf) {
            Leaf leaf = (Leaf) node;
            System.out.println("Leaf Level " + level + ": " + leaf.toString());
        } else {
            NonLeaf nonleaf2 = (NonLeaf) node;
            System.out.println("Non-leaf Level " + level + ": " + nonleaf2.toString());

            for (int i = 0; i < nonleaf2.children.length; i++) {
                Node child = nonleaf2.children[i];
                if (child != null) {
                    printOctree(child, level + 1);
                }
            }
        }
    }



    private static void printOctree(Node node, String prefix, boolean isTail) {
        String currentNode = prefix + (isTail ? "└── " : "├── ");

        // Print current node endpoint
        //System.out.println(currentNode +" "+ node);
        System.out.println(currentNode +" "+ (node instanceof Leaf ?("LEAF "):("NON LEAF ")) +"RANGE: "+node.endPoints[0]+ " "+node.endPoints[1]);

        if (node instanceof Leaf) {
            // Get leaf node's entries
            Leaf leaf=(Leaf) node;
            Hashtable<Point, ArrayList<Refrence>> leafContents = leaf.getPointRefrenceHashtable();
            int count = 0;

//            for (Point key : leafContents.keySet()) {
//                count++;
//                ArrayList<Refrence> value = leafContents.get(key);

                // Print leaf entries
               // System.out.println(prefix + (isTail ? "    " : "│   ") + "Entry " + count + ": "+"KEY: " + key + "--> " + leaf.toString());

             System.out.println(prefix + (isTail ? "    " : "│   ") + " LEAF HASHTABLE " +leaf.pointRefrenceHashtable);


        }
         else {
            // Recursively print child nodes
            NonLeaf nonLeaf=(NonLeaf)node;
            Node[] children = nonLeaf.children;
            int index = 0;

            for (Node child : children) {
                index++;
                boolean isLast = index == children.length;

                String newPrefix = prefix + (isTail ? "    " : "│   ");
                printOctree(child, newPrefix, isLast);
            }
        }
    }





}
