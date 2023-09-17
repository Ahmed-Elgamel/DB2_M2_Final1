public class DBAppException extends Exception{


    public DBAppException()
    {
        super();
    }
    public DBAppException(String message)
    {
        //super(message);
        System.out.println(message);
    }


}
