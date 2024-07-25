package json;

import java.util.concurrent.ThreadLocalRandom;

public class WorkerService {
    public static void main(String[] args) {
        new Worker<TaskStatus, Task>("localhost", "json-status", TaskStatus.class) {
            @Override
            public Task process(TaskStatus task) throws InterruptedException {
                System.out.println("1 : " + task.id);
                return WorkerService.process(task, "1");
            }
        }.init();


        new Worker<TaskStatus, Task>("localhost", "json-status", TaskStatus.class) {
            @Override
            public Task process(TaskStatus task) throws InterruptedException {
                System.out.println("2 : " + task.id);
                return WorkerService.process(task, "2");
            }
        }.init();
    }


    static Task process(TaskStatus taskS, String worker) throws InterruptedException {
        if(!taskS.state.equals("INITIAL"))
            return null;
        int min = 1;
        int max = 5;
        int randomNum = ThreadLocalRandom.current().nextInt(min, max + 1);
        Thread.sleep(randomNum * 1000L);

        Task rTask = new Task();
        rTask.id = taskS.id;
        rTask.action = taskS.action;
        rTask.worker = worker;
        rTask.state = "OK";

        System.out.println("process : " + worker);
        return rTask;
    }
}
