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

   private static String TAG = "NN";
   private HashMap<Integer, String> handle_filename_map;
   private HashMap<String, ArrayList<Integer>> filename_block_map;
   private HashMap<Integer, ArrayList<Integer>> block_datanode_map;
   public int blockNum, fileNum;
   private static int dataNodeNum = 4;
   private static String[] dataNodeIPs = {"127.0.0.1","127.0.0.1","127.0.0.1","127.0.0.1"};
   private static int[] dataNodePorts = {1099,1099,1099,1099};

   public NameNode() {
      blockNum = 0;
      fileNum = 0;
      handle_filename_map = new HashMap<Integer, String>();
      filename_block_map = new HashMap<String, ArrayList<Integer>>();
      block_datanode_map = new HashMap<Integer, ArrayList<Integer>>();
   }

   public byte[] openFile(byte[] inp) throws RemoteException{
      try {
         Hdfs.OpenFileRequest openFileRequest =Hdfs.OpenFileRequest.parseFrom(inp);
         String filename = openFileRequest.getFileName();
         boolean forRead = openFileRequest.getForRead();
         
         byte[] openFileResponseBytes;
         handle_filename_map.put(fileNum, filename);
                
         Hdfs.OpenFileResponse.Builder openFileResponseBuilder = Hdfs.OpenFileResponse.newBuilder();
         openFileResponseBuilder.setStatus(1);
         openFileResponseBuilder.setHandle(fileNum);
         if(filename_block_map.get(filename)!=null)
            for(int i : filename_block_map.get(filename))
               openFileResponseBuilder.addBlockNums(i);
          fileNum++;
         return openFileResponseBuilder.build().toByteArray();
      }
      catch(Exception e){System.out.println("Unable to open file at name node\n");}

      return null;
   }

   public byte[] closeFile(byte[] inp ) throws RemoteException{
      return null;
   }

   public byte[] getBlockLocations(byte[] inp ) throws RemoteException{
      return null;
   }

   public byte[] assignBlock(byte[] inp ) throws RemoteException{
      byte[] assignBlockResponseBytes = null;
      try {
         Hdfs.AssignBlockRequest assignBlockRequest = Hdfs.AssignBlockRequest.parseFrom(inp);
         int handle=assignBlockRequest.getHandle();
         String filename = (String) handle_filename_map.get(handle);
         if(filename_block_map.get(filename)!=null)
            filename_block_map.get(filename).add(blockNum);
         else
            filename_block_map.put(filename, new ArrayList<Integer>(Arrays.asList(blockNum)));

         Hdfs.BlockLocations.Builder blockLocationsBuilder = Hdfs.BlockLocations.newBuilder();
         
         Hdfs.DataNodeLocation.Builder dataNodeLocationBuilder = Hdfs.DataNodeLocation.newBuilder();

         int datanode1, datanode2;
         datanode1 = new Random().nextInt(dataNodeNum);
         do {
            datanode2 = new Random().nextInt(dataNodeNum);
         } while(datanode2==datanode1);
         
         System.out.println(datanode1 + " " + datanode2);
         blockLocationsBuilder.setBlockNumber(blockNum);

         dataNodeLocationBuilder.setIp(dataNodeIPs[datanode1]);
         dataNodeLocationBuilder.setPort(dataNodePorts[datanode1]);
         blockLocationsBuilder.addLocations(dataNodeLocationBuilder.build());

         dataNodeLocationBuilder.setIp(dataNodeIPs[datanode2]);
         dataNodeLocationBuilder.setPort(dataNodePorts[datanode2]);
         blockLocationsBuilder.addLocations(dataNodeLocationBuilder.build());

         block_datanode_map.put(blockNum, new ArrayList<Integer>(Arrays.asList(datanode1, datanode2)));
         blockNum++;
         
         Hdfs.AssignBlockResponse.Builder assignBlockResponseBuilder = Hdfs.AssignBlockResponse.newBuilder();
         assignBlockResponseBuilder.setStatus(1);
         assignBlockResponseBuilder.setNewBlock(blockLocationsBuilder.build());
         assignBlockResponseBytes = assignBlockResponseBuilder.build().toByteArray(); 
      } catch(Exception e){
         System.out.println("Exception while assigning block at name server");
         e.printStackTrace();
      }
      return assignBlockResponseBytes;
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
