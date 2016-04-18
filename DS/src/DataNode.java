import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;
import com.google.protobuf.ByteString;

public class DataNode implements IDataNode {

   public DataNode() {}
   private static String TAG = "DN";
   private static String NN_IP = "10.1.35.147";
   private static int NN_PORT = 1099;
   private static final int heartBeatReturnCode = 1;
   private static final String BLOCK_REPORT = "block_report.txt";
   private static int ID;
   private static final int block_size = 32*1024*1024;

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

   static class HeartBeatThread extends Thread{

	  public HeartBeatThread(){
	  }

	  public void run(){
	 try{
		while(true){
		   Hdfs.HeartBeatRequest.Builder heartBeatRequestBuilder = Hdfs.HeartBeatRequest.newBuilder().setId(ID);
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

   public byte[] readBlock(byte[] inp) throws RemoteException{
	  File dir = new File("Blocks");
	  Hdfs.ReadBlockResponse.Builder rbr_builder = Hdfs.ReadBlockResponse.newBuilder().setStatus(1);
	  try{
	 int blk_num = Hdfs.ReadBlockRequest.parseFrom(inp).getBlockNumber();
	 File block = new File(dir, String.valueOf(blk_num));
	 FileInputStream fis = new FileInputStream(block);
	 byte[] blk = new byte[block_size];
	 int bytes;

	 while((bytes = fis.read(blk)) != -1){
		ByteString data = ByteString.copyFrom(blk);
		rbr_builder.addData(data);
	 }

	  } catch( Exception e) {
	 e.printStackTrace();
	  }

	  return rbr_builder.build().toByteArray();
   }

   public byte[] writeBlock(byte[] inp) throws RemoteException{

		try{
			File dir = new File("Blocks");
			Hdfs.WriteBlockRequest writeBlockRequest = Hdfs.WriteBlockRequest.parseFrom(inp);


			int blockNum = writeBlockRequest.getBlockInfo().getBlockNumber();
			File blockFile = new File(dir, String.valueOf(blockNum));
			FileOutputStream fos = new FileOutputStream(blockFile);

            List<ByteString> dataString = writeBlockRequest.getDataList();
            for(ByteString byteString : dataString)
                fos.write(byteString.toByteArray());

			fos.close();

            File report = new File(BLOCK_REPORT);
            FileWriter fw = new FileWriter(report.getName(), true);

            BufferedWriter bw = new BufferedWriter(fw);

            bw.write(Integer.toString(blockNum));
            bw.newLine();
            bw.close();


			return Hdfs.WriteBlockResponse.newBuilder().setStatus(1).build().toByteArray();
		} catch( Exception e) {
			e.printStackTrace();
		}

		return null;
   }

   public static void main(String args[]) {

      System.setProperty("java.rmi.server.hostname", "10.1.35.147");
	  ID = Integer.parseInt(args[0]);

	  File b = new File("Blocks");
	  if(!b.exists())
	     b.mkdirs();

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
