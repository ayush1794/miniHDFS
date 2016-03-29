import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.RemoteException;

public class DataNode implements IDataNode {

    public DataNode() {}
    private static String TAG = "DN";

    public byte[] readBlock(byte[] inp) throws RemoteException{
       return null;
    }

    public byte[] writeBlock(byte[] inp) throws RemoteException{
      return null;
    }

    public static void main(String args[]) {

        try {
            DataNode obj = new DataNode();
            IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(obj, 0);

            // Bind the remote object's stub in the registry
            Registry registry = LocateRegistry.getRegistry();
            registry.bind(TAG, stub);

            System.err.println("DataNode ready");
        } catch (Exception e) {
            System.err.println("DataNode exception: " + e.toString());
            e.printStackTrace();
        }
    }
}
