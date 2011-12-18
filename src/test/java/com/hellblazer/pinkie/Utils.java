package com.hellblazer.pinkie;

import static junit.framework.Assert.fail;

public class Utils {

    public static interface Condition {
        boolean value();
    }

    public static void waitFor(String reason, Condition condition,
                               long timeout, long interval)
                                                           throws InterruptedException {
        long target = System.currentTimeMillis() + timeout;
        while (!condition.value()) {
            if (target < System.currentTimeMillis()) {
                fail(reason);
            }
            Thread.sleep(interval);
        }
    }

}
