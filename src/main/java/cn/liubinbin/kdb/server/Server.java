package cn.liubinbin.kdb.server;

public class Server {

    public void start(){

    }

    public void localMock(){
        System.out.println("local mock");
    }

    public static void main(String[] args) {
        // new Server().start();
        new Server().localMock();
    }
    
}
