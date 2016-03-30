import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;

public class DataNode implements IDataNode {

   public DataNode() {}
   private static String TAG = "DN";
   private static String NN_IP;
   private static int NN_PORT = 1099;
   private static final int heartBeatReturnCode = 1;
   private static final String BLOCK_REPORT = "block_report.txt";
   private static int ID;

   static class BlockReportThread extends Thread{

      public BlockReportThread(){
      }

      public void run(){
	 while(true){
	    File blk_rpt = new File(BLOCK_REPORT);
	    String data = "";
	    if (blk_rpt.exists() && blk_rpt.length()!=0){
	       try{
		  BufferedReader br = new BufferedReader(new FileReader(blk_rpt));
		  String blk_num;
		  Hdfs.BlockReportRequest.Builder blk_rpt_req_builder = Hdfs.BlockReportRequest.newBuilder().setId(ID);
		  while((blk_num = br.readLine())!=null){
		     blk_rpt_req_builder.addBlockNumbers(Integer.parseInt(blk_num));
		  }
		  br.close();

		  Registry registry = LocateRegistry.getRegistry(NN_IP, NN_PORT);
		  INameNode stub = (INameNode) registry.lookup("NN");
		  byte[] blockReportResponse = stub.blockReport(blk_rpt_req_builder.build().toByteArray());

		  Hdfs.BlockReportResponse res = Hdfs.BlockReportResponse.parseFrom(blockReportResponse);
		  System.err.println("Block Report Response from NN " + String.valueOf(res.getStatusCount()));
		  Thread.sleep(10000);

	       } catch (Exception e) {
		  e.printStackTrace();
	       }
	    }
	    else {
	       System.err.println("No block report");
	       try{
		  Thread.sleep(10000);
	       } catch (Exception e) {
		  e.printStackTrace();
	       }
	    }
	 }
      }
   }
}

   static class HeartBeatThread extends Thread{

      public HeartBeatThread(){
      }

      public void run(){
	 try{
	    while(true){
	       Hdfs.HeartBeatRequest.Builder heartBeatRequestBuilder = Hdfs.HeartBeatRequest.newBuilder().setId(heartBeatReturnCode);
	       Registry registry = LocateRegistry.getRegistry(NN_IP, NN_PORT);
	       INameNode stub = (INameNode) registry.lookup("NN");
	       byte[] heartBeatResponse = stub.heartBeat(heartBeatRequestBuilder.build().toByteArray());

	       Hdfs.HeartBeatResponse res = Hdfs.HeartBeatResponse.parseFrom(heartBeatResponse);
	       System.err.println("Heart Beat Response from NN " + String.valueOf(res.getStatus()));
	       Thread.sleep(10000);
	    }

	 } catch (Exception e) {
	    e.printStackTrace();
	 }
      }
   }

   public synchronized byte[] readBlock(byte[] inp) throws RemoteException{
      File dir = new File("Blocks");
      byte[] data = null;
      try{
	 int blk_num = Hdfs.ReadBlockRequest.parseFrom(inp).getBlockNumber();
	 File block = new File(dir, String.valueOf(blk_num));
	 FileInputStream fis = new FileInputStream(block);
      } catch( Exception e) {
	 e.printStackTrace();
      }
      return null;
   }

   public synchronized byte[] writeBlock(byte[] inp) throws RemoteException{
      return null;
   }

   public static void main(String args[]) {

      ID = Integer.parseInt(args[0]);

      try {

	 DataNode obj = new DataNode();
	 IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(obj, 0);

	 // Bind the remote object's stub in the registry
	 Registry registry = LocateRegistry.getRegistry();
	 registry.bind(TAG, stub);

	 System.err.println("DataNode ready");
	 HeartBeatThread hbt = new HeartBeatThread();
	 hbt.start();
	 BlockReportThread brt = new BlockReportThread();
	 brt.start();

      } catch (Exception e) {
	 System.err.println("DataNode exception: " + e.toString());
	 e.printStackTrace();
      }
   }
}
