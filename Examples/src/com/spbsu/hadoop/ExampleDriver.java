package com.spbsu.hadoop;

import org.apache.hadoop.util.ProgramDriver;

/**
 * User: svasilinets
 * Date: 27.03.12
 * Time: 10:54
 */
public class ExampleDriver {

    public static void main(String[] args) {
        ProgramDriver programDriver = new ProgramDriver();
        try {
            programDriver.addClass("letters", FirstLetterCounter.class, "calc first letter ");
            programDriver.addClass("ip", SessionDuration.class, "ipcs");

            programDriver.driver(args);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

    }
}
