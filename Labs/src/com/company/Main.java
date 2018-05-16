package com.company;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.lang.ProcessBuilder;

public class Main {

    public static void main(String[] args) {
        etape1();

    }

    public static void etape1(){
        // TP Etape 1 - monothread wordcount
        FileReader fr = null;
        Scanner sc = null;
        HashMap<String, Integer> wordCount = null;
        HashMap<Integer, List<String>> invIndex = null;
        List<String> keys;
        long startTime, endTime, totalTime;

        try {
            String path = "~/src/procedure_civile.txt";
            fr = new FileReader(path.replaceFirst("^~",System.getProperty("user.dir")));
            sc = new Scanner(fr);

            wordCount = new HashMap<>();
            invIndex = new HashMap<>();

            startTime = System.currentTimeMillis();

            // Wordcount
            while (sc.hasNext()){
                String word = sc.next();
                wordCount.merge(word, 1, Integer::sum);
            }

            endTime   = System.currentTimeMillis();
            totalTime = endTime - startTime;

            System.out.println("Wordcount time: "+totalTime);

            // Get keys
            keys = new ArrayList<>(wordCount.keySet());

            // Inverted Index
            for (String key : keys){
                Integer newIndex = wordCount.get(key);
                if (!invIndex.containsKey(newIndex)){
                    invIndex.put(newIndex, new ArrayList<>(Collections.singletonList(key)));
                } else {
                    List<String> strings = invIndex.get(newIndex);
                    strings.add(key);
                    invIndex.replace(newIndex, strings);
                }
            }

            // Print on screen - ordered by frequency and alphabetically
            List<Integer> indices = new ArrayList<>(invIndex.keySet());
            Comparator<Integer> cmp = Collections.reverseOrder();
            Collections.sort(indices, cmp);
            for (Integer i : indices){
                List<String> strings = new ArrayList<>(invIndex.get(i));
                Collections.sort(strings);
                for (String string : strings)
                    System.out.println(string+" "+i);
            }

        } catch (Exception e){
            e.printStackTrace();
        } finally {
            try {
                // Cleanup
                sc.close();
                fr.close();
                wordCount.clear();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
