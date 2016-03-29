import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

public class Client {

   private Client() {}
   private static String NN_IP;
   private static int NN_PORT = 1099;
   private static Registry NN_Registry, DN_Registry;
   private static String GET = "get";
   private static String PUT = "put";
   private static String LIST = "list";

      public static void main(String[] args){

	 Scanner scanner = new Scanner(System.in);
	 Hdfs.OpenFileRequest.Builder ofr;
	 // String host = (args.length < 1) ? null : args[0];
	 try {
	    NN_Registry = LocateRegistry.getRegistry(NN_IP, NN_PORT);
	    INameNode stub = (INameNode) NN_Registry.lookup("NN");
	    while (true) {
	       String command = scanner.next();
	       if (command.equals(GET)){
		  String fileName = scanner.next();
		  System.err.println(fileName);
	       }
	       else if (command.equals(PUT)){
		  String fileName = scanner.next();
		  System.err.println(fileName);
	       }
	       else if (command.equals(LIST)){
		  System.err.println(LIST);
	       }
	       else{
		  System.err.println("Invalid command");
	       }
	    }
	 } catch (Exception e) {
	    System.err.println("Client exception: " + e.toString());
	    e.printStackTrace();
	 }
      }
}
