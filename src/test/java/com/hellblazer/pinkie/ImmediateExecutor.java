package com.hellblazer.pinkie;

import java.util.concurrent.Executor;

public class ImmediateExecutor implements Executor {

    @Override
    public void execute(Runnable action) {
        action.run();
    }

}
