import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.rmi.server.UnicastRemoteObject;
import java.io.*;
import java.rmi.RemoteException;

public class NameNode implements INameNode {

   private static String TAG = "NN", FILE_LIST = "file_list.txt";
   private HashMap<Integer, String> handle_filename_map;
   private static HashMap<String, ArrayList<Integer>> filename_block_map;
   private static HashMap<Integer, ArrayList<Integer>> block_datanode_map;
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
      try{
	 Hdfs.CloseFileRequest closeFileRequest = Hdfs.CloseFileRequest.parseFrom(inp);
	 int handle = closeFileRequest.getHandle();


	 File report = new File(FILE_LIST);

	 FileWriter fw = new FileWriter(report.getName(), true);

	 BufferedWriter bw = new BufferedWriter(fw);

	 String filename = (String) handle_filename_map.get(handle);
	 ArrayList<Integer> blockList = filename_block_map.get(filename);

	 bw.write(filename+" ");
	 for(int i : blockList) {
	    bw.write(Integer.toString(i)+" ");
	 }
	 bw.newLine();

	 bw.close();
      } catch( Exception e )
      {
	 e.printStackTrace();
      }
      return null;

   }

   public byte[] getBlockLocations(byte[] inp ) throws RemoteException{
      try{
	 Hdfs.BlockLocationRequest blr = Hdfs.BlockLocationRequest.parseFrom(inp);
	 int block_number = blr.getBlockNum();

	 Hdfs.BlockLocationResponse.Builder blr_builder = Hdfs.BlockLocationResponse.newBuilder().setStatus(1);
	 Hdfs.BlockLocations.Builder bl_builder = Hdfs.BlockLocations.newBuilder().setBlockNumber(block_number);
	 for (int node : block_datanode_map.get(block_number)){
	    Hdfs.DataNodeLocation.Builder dnl_builder = Hdfs.DataNodeLocation.newBuilder().setIp(dataNodeIPs[node]).setPort(dataNodePorts[node]);
	    bl_builder.addLocations(dnl_builder.build());
	 }
	 blr_builder.setBlockLocations(bl_builder.build());
	 return blr_builder.build().toByteArray();
      } catch (Exception e) {
	 e.printStackTrace();
      }
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
	 e.printStackTrace();
      }
      return assignBlockResponseBytes;
   }

   public byte[] list(byte[] inp ) throws RemoteException{
      try{
	 Hdfs.ListFilesResponse.Builder lfr_builder = Hdfs.ListFilesResponse.newBuilder().setStatus(1);
	 for(String fileName : filename_block_map.keySet()){
	    lfr_builder.addFileNames(fileName);
	 }
	 return lfr_builder.build().toByteArray();
      } catch (Exception e) {
	 e.printStackTrace();
      }
      return null;
   }

   public byte[] blockReport(byte[] inp ) throws RemoteException{
      try{
	 Hdfs.BlockReportRequest req = Hdfs.BlockReportRequest.parseFrom(inp);
	 int datanode_id = req.getId();
	 int num_blks = req.getBlockNumbersCount();

	 for(int i=0;i<num_blks;i++){
	    if(block_datanode_map.get(req.getBlockNumbers(i)) == null){
	       block_datanode_map.put(req.getBlockNumbers(i), new ArrayList<Integer>(Arrays.asList(datanode_id)));
	    }
	    else{
	       if (!block_datanode_map.get(req.getBlockNumbers(i)).contains(datanode_id))
		  block_datanode_map.get(req.getBlockNumbers(i)).add(datanode_id);
	    }
	 }

	 Hdfs.BlockReportResponse.Builder brr_builder = Hdfs.BlockReportResponse.newBuilder().addStatus(1);
	 return brr_builder.build().toByteArray();
      } catch (Exception e) {
	 e.printStackTrace();
      }
      return null;
   }

   public byte[] heartBeat(byte[] inp ) throws RemoteException{
      try{
	 Hdfs.HeartBeatRequest req = Hdfs.HeartBeatRequest.parseFrom(inp);
	 int id = req.getId();
	 System.err.println("HeartBeat received from DN : " + String.valueOf(id));

	 Hdfs.HeartBeatResponse.Builder hbr_builder = Hdfs.HeartBeatResponse.newBuilder().setStatus(1);
	 return hbr_builder.build().toByteArray();
      } catch (Exception e) {
	 e.printStackTrace();
      }
      return null;
   }

   public static void main(String args[]) {
      //TODO : set hostname property
      File report = new File(FILE_LIST);

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

      try {
	 BufferedReader br = new BufferedReader(new FileReader(report));
	 String line, filename;
	 filename = "";
	 while ((line = br.readLine()) != null) {
	    int blockNumber;

	    String[] fileBlocks = line.split(" ");
	    filename = fileBlocks[0];
	    ArrayList<Integer> blocks = new ArrayList<Integer>();
	    for(int i=1;i<fileBlocks.length;i++)
	       blocks.add(Integer.parseInt(fileBlocks[i]));
	    filename_block_map.put(filename, blocks);
	 }
      } catch(Exception e){
	 e.printStackTrace();
      }

   }
}
