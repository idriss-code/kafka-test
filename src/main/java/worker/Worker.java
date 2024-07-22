package worker;


public interface Worker {

    void init();
    String process(String task);
}
