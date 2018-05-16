package com.SLR207;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder;
import java.util.concurrent.TimeUnit;

public class Master {

    public static void main(String[] args) {

        String output;
        long startTime, endTime, totalTime;

        ProcessBuilder pb = new ProcessBuilder("java", "-jar", "/tmp/castelluccio/slave.jar");
        // Redirect Error to stream
        pb.redirectErrorStream(true);
        pb.inheritIO();

        try {
            startTime = System.currentTimeMillis();
            // Start the process
            Process p = pb.start();

            boolean b = p.waitFor(3, TimeUnit.SECONDS);
            if (!b) {
                // Timeout hit
                p.destroy();
                throw new InterruptedException();
            }

            // Normal termination

            // Get the word from stream
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder builder = new StringBuilder(); String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(System.getProperty("line.separator"));
            }
            String result = builder.toString();

            endTime = System.currentTimeMillis();

            // Print output
            System.out.println(result);
            System.out.println("MASTER: "+(endTime-startTime)+" ms");

        } catch (InterruptedException e) {
            System.out.println("Timeout hit. Try again.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
