package json;

public class WorkerService {
    public static void main(String[] args) {
        new Worker<TaskStatus, Task>("localhost", "json-status", TaskStatus.class) {
            @Override
            public Task process(TaskStatus task) throws InterruptedException {
                Thread.sleep(3000);
                System.out.println("1 : " + task.id);
                return ttt(task, "1");
            }
        }.init();


        new Worker<TaskStatus, Task>("localhost", "json-status", TaskStatus.class) {
            @Override
            public Task process(TaskStatus task) throws InterruptedException {
                Thread.sleep(5000);
                System.out.println("2 : " + task.id);
                return ttt(task, "2");
            }
        }.init();
    }


    static Task ttt(TaskStatus taskS, String worker) {
        Task rTask = new Task();
        rTask.id = taskS.id;
        rTask.action = taskS.action;

        rTask.state = "OK " + worker;
        return rTask;
    }
}
