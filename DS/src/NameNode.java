import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;

public class NameNode implements INameNode {

   public NameNode() {}
   private static String TAG = "NN";

   public byte[] openFile(byte[] inp) throws RemoteException{
      return null;
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
