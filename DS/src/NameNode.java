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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
public class NameNode implements INameNode {

   public NameNode() {}
   private static String TAG = "NN";
   private HashMap<Integer, String> block_handle_map;
   private HashMap<String, ArrayList<Integer>> handle_block_map;
   public int blockNum, fileNum;

   public byte[] openFile(byte[] inp) throws RemoteException{
      try {
         Hdfs.OpenFileRequest openFileRequest =Hdfs.OpenFileRequest.parseFrom(inp);
         String filename = openFileRequest.getFileName();
         boolean forRead = openFileRequest.getForRead();
         
         byte[] openFileResponseBytes;
         block_handle_map.put(blockNum, filename);
                  
         Hdfs.OpenFileResponse.Builder openFileResponseBuilder = Hdfs.OpenFileResponse.newBuilder().setStatus(1).setHandle(fileNum);
         for(int i : handle_block_map.get(filename))
             openFileResponse.addBlockNums(i);
          fileNum++;
         return openFileResponseBuilder.build().toByteArray();
      }
      catch(Exception e){System.out.println("Unable to open file at name node\n");}

      return Hdfs.OpenFileResponse.newBuilder().setStatus(0).setHandle(-1).build().toByteArray();
   }

   public byte[] closeFile(byte[] inp ) throws RemoteException{
      return null;
   }

   public byte[] getBlockLocations(byte[] inp ) throws RemoteException{
      return null;
   }

   public byte[] assignBlock(byte[] inp ) throws RemoteException{
      return null;
   }

   public byte[] list(byte[] inp ) throws RemoteException{
      return null;
   }

   public byte[] blockReport(byte[] inp ) throws RemoteException{
      return null;
   }

   public byte[] heartBeat(byte[] inp ) throws RemoteException{
      return null;
   }

   public static void main(String args[]) {
      //TODO : set hostname property

      try {
	 NameNode obj = new NameNode();
	 INameNode stub = (INameNode) UnicastRemoteObject.exportObject(obj, 0);

	 // Bind the remote object's stub in the registry
	 Registry registry = LocateRegistry.getRegistry();
	 registry.bind(TAG, stub);
	 System.err.println("NameNode ready");

      } catch (Exception e) {
	 System.err.println("NameNode exception: " + e.toString());
	 e.printStackTrace();
      }
   }
}
