import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.rmi.server.UnicastRemoteObject;
import java.util.Random;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.rmi.RemoteException;


public class Client {

   private Client() {}
   private static String NN_IP;
   private static int NN_PORT = 1099;
   private static Registry NN_Registry, DN_Registry;
   private static String GET = "get";
   private static String PUT = "put";
   private static String LIST = "list";
   private int blockSize = 33554432; // 32 Mb

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

		  //openfile
		  byte[] openFileRequestBytes = Hdfs.OpenFileRequest.newBuilder().setFileName(fileName).setForRead(false).build().toByteArray();
		  byte[] openFileResponseBytes = stub.openFile(openFileRequestBytes);
		  Hdfs.OpenFileResponse openFileResponse = Hdfs.OpenFileResponse.parseFrom(openFileResponseBytes);

		  if(openFileResponse.getStatus()) {
		  	byte[] readBytes = new byte[blockSize];
		  	int numBytes;

            FileInputStream input = new FileInputStream(new File(filename));
            while ((read_bytes = fis.read(w)) != -1) {
				byte[] assignBlockRequestBytes = Hdfs.AssignBlockRequest.newBuilder().setHandle(openFileResponse.getHandle()).build().toByteArray();
				byte[] assignBlockResponseBytes = tempStub.assignBlock(assignBlockRequestBytes);
				Hdfs.AssignBlockResponse assignBlockResponse = Hdfs.AssignBlockResponse.parseFrom(assignBlockResponseBytes);
            }

		  }

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
