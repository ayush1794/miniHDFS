import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;
import com.google.protobuf.ByteString;

public class Client {

   private Client() {}
   private static String NN_IP;
   private static int NN_PORT = 1099;
   private static Registry NN_Registry, DN_Registry;
   private static String GET = "get";
   private static String PUT = "put";
   private static String LIST = "list";
   private static int blockSize = 33554432; // 32 Mb

   private static byte[] openFile(INameNode stub, String fileName, boolean forRead) throws Exception{

      Hdfs.OpenFileRequest.Builder openFileRequestBuilder = Hdfs.OpenFileRequest.newBuilder();
      openFileRequestBuilder.setFileName(fileName);
      openFileRequestBuilder.setForRead(forRead);
      byte[] openFileRequestBytes = openFileRequestBuilder.build().toByteArray();
      return stub.openFile(openFileRequestBytes);
   }

   public static void main(String[] args){

      Scanner scanner = new Scanner(System.in);
      // String host = (args.length < 1) ? null : args[0];
      try {
	 NN_Registry = LocateRegistry.getRegistry();
	 INameNode stub = (INameNode) NN_Registry.lookup("NN");
	 while (true) {
	    String command = scanner.next();
	    if (command.equals(GET)){
	       String fileName = scanner.next();
	       System.err.println(fileName);

	       byte[] openFileResponseBytes = openFile(stub, fileName, true);
	       if(openFileResponseBytes != null){
		  Hdfs.OpenFileResponse ofr = Hdfs.OpenFileResponse.parseFrom(openFileResponseBytes);
		  int handle = ofr.getHandle();
		  int blk_count = ofr.getBlockNumsCount();

		  File file = new File(fileName);
		  FileOutputStream fs = new FileOutputStream(file);

		  for(int i=0;i<blk_count;i++){
		     Hdfs.BlockLocationRequest.Builder blr_builder = Hdfs.BlockLocationRequest.newBuilder().setBlockNum(ofr.getBlockNums(i));
		     byte[] res = stub.getBlockLocations(blr_builder.build().toByteArray());

		     Hdfs.BlockLocationResponse blr = Hdfs.BlockLocationResponse.parseFrom(res);
		     if(blr.getStatus() == 1){
			Hdfs.BlockLocations bl = blr.getBlockLocations();
			String DN_IP = bl.getLocations(0).getIp();
			int DN_PORT = bl.getLocations(0).getPort();
			Registry reg = LocateRegistry.getRegistry(DN_IP,DN_PORT);
			IDataNode stub1 = (IDataNode) reg.lookup("DN");

			Hdfs.ReadBlockRequest.Builder rbr_builder = Hdfs.ReadBlockRequest.newBuilder().setBlockNumber(ofr.getBlockNums(i));
			byte[] resp = stub1.readBlock(rbr_builder.build().toByteArray());

			Hdfs.ReadBlockResponse rbr = Hdfs.ReadBlockResponse.parseFrom(resp);
			ByteString data = rbr.getData(0);
			fs.write(data.toByteArray());
		     }
		  }
		  fs.close();
	       }
	       //TODO : add close call.
	    }

	    else if (command.equals(PUT)){
	       String fileName = scanner.next();
	       System.err.println(fileName);

	       byte[] openFileResponseBytes = openFile(stub, fileName, false);
	       if(openFileResponseBytes!=null) {
		  Hdfs.OpenFileResponse openFileResponse = Hdfs.OpenFileResponse.parseFrom(openFileResponseBytes);

		  byte[] readBytes = new byte[blockSize];
		  int numBytes;

		  FileInputStream input = new FileInputStream(new File(fileName));
		  IDataNode dnStub = (IDataNode) NN_Registry.lookup("DN");
		  while ((numBytes = input.read(readBytes)) != -1) {
		    Hdfs.AssignBlockRequest.Builder assignBlockRequestBuilder = Hdfs.AssignBlockRequest.newBuilder();
		    assignBlockRequestBuilder.setHandle(openFileResponse.getHandle());
		    byte[] assignBlockRequestBytes = assignBlockRequestBuilder.build().toByteArray();
		    byte[] assignBlockResponseBytes = stub.assignBlock(assignBlockRequestBytes);
		    Hdfs.AssignBlockResponse assignBlockResponse = Hdfs.AssignBlockResponse.parseFrom(assignBlockResponseBytes);
		    //Hdfs.BlockLocations blockLocations = assignBlockResponse.getNewBlock();

		    Hdfs.WriteBlockRequest.Builder writeBlockRequestBuilder = Hdfs.WriteBlockRequest.newBuilder();
		    writeBlockRequestBuilder.addData(ByteString.copyFrom(readBytes));
		    writeBlockRequestBuilder.setBlockInfo(assignBlockResponse.getNewBlock());
		    byte[] writeBlockResponseBytes = dnStub.writeBlock(writeBlockRequestBuilder.build().toByteArray());
			Hdfs.WriteBlockResponse writeBlockResponse = Hdfs.WriteBlockResponse.parseFrom(writeBlockResponseBytes);
		  }

	       }

	    }
	    else if (command.equals(LIST)){
	       System.err.println(LIST);
	       try{
		  NN_Registry = LocateRegistry.getRegistry();
		  INameNode stub1 = (INameNode) NN_Registry.lookup("NN");
		  byte[] res = stub1.list(null);
		  Hdfs.ListFilesResponse resp = Hdfs.ListFilesResponse.parseFrom(res);
		  int count = resp.getFileNamesCount();
		  for(int i=0;i<count;i++)
		     System.err.println(resp.getFileNames(i));
	       } catch (Exception e) {
		  e.printStackTrace();
	       }
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
