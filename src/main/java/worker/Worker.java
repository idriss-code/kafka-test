package worker;

import com.dustmobile.backend.service.sim.queue.Exception.AckException;
import com.dustmobile.backend.service.sim.queue.Exception.NoAckException;

public interface Worker {

    void init();
    String process(String task) throws AckException, NoAckException;
}
