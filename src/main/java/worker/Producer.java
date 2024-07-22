package worker;

public interface Producer {
     void send(String message) throws Exception;
     String syncSend(String message) throws Exception;

}
