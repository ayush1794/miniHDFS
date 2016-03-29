import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class Client {

    private Client() {}
    private static String NN_IP;
    private static int NN_PORT = 1099;
    private static Registry NN_Registry, DN_Registry;

    public static void main(String[] args){

       // String host = (args.length < 1) ? null : args[0];
        try {
            NN_Registry = LocateRegistry.getRegistry(NN_IP, NN_PORT);
            INameNode stub = (INameNode) NN_Registry.lookup("NN");

        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        }
    }
}
