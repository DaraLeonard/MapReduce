package MapReduce;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class MapReduce_Threadpool {

    private static PrintWriter pW;

	public static void main(String[] args) throws IOException {
    	// Set up the output file by creating a PrintWriter object to allow the program's
		// output to be written to a file
		String fileName = "DataOut.txt";
    	pW = new PrintWriter(fileName);
    	
    	//Start the programStartTime
    	final long programStartTime = System.currentTimeMillis();
    	Map<String, String> input = new ConcurrentHashMap<String, String>();
    	final long loadDataStartTime = System.currentTimeMillis();
    	
    	//Take the input arguments, starting from args[1], and add them to a ConcurrentHashMap
    	for(int i = 1; i<args.length; i++){
    		File file = new File(args[i]);
    		String[] map = ReadTextFile.readFile(file);
    		String name = map[0];
    		String content = map[1];
    		input.put(name, content);
    	}

    	final long loadDataPhaseDuration = System.currentTimeMillis() - loadDataStartTime;
        
    	
        // APPROACH #3: Distributed MapReduce
        final Map<String, Map<String, Integer>> output = new ConcurrentHashMap<String, Map<String,Integer>>();

        // Create threadpool of the size defined by args[0]
        int threadPoolSize = Integer.parseInt(args[0]);
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        
        //Print out the number of threads, and time taken to prepare data to file and console.
        System.out.println("Number of threads: " + threadPoolSize);
        System.out.println("Time taken for loading data phase : " + loadDataPhaseDuration + "ms");
        pW.println("Number of threads: " + threadPoolSize);
        pW.println("Time taken for loading data phase : " + loadDataPhaseDuration + "ms");
        
        // MAP:
        //Start timer for mapping phase
		final long mapPhaseStartTime = System.currentTimeMillis();
        final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
        final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
            @Override
            public synchronized void mapDone(String file, List<MappedItem> results) {
                mappedItems.addAll(results);
            }
        };

        Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
        while(inputIter.hasNext()) {
            Map.Entry<String, String> entry = inputIter.next();
            final String file = entry.getKey();
            final String contents = entry.getValue();

            executor.execute(() -> map(file, contents, mapCallback));
        }
        // Wait here until mapping phase is over
        executor.shutdown();
        while(!executor.isTerminated());

        final long mapPhaseDuration = System.currentTimeMillis() - mapPhaseStartTime;
        
        //Print time taken for for mapping phase
		System.out.println("Time taken for mapping phase : " + mapPhaseDuration + "ms");
		pW.print("Time taken for mapping phase : " + mapPhaseDuration + "ms");     
        
		// GROUP:
		//Start time for mapping phase
		final long groupPhaseStartTime = System.currentTimeMillis();

        Map<String, List<String>> groupedItems = new ConcurrentHashMap<String, List<String>>();

        Iterator<MappedItem> mappedIter = mappedItems.iterator();
        while(mappedIter.hasNext()) {
            MappedItem item = mappedIter.next();
            String word = item.getWord();
            String file = item.getFile();
            List<String> list = groupedItems.get(word);
            if (list == null) {
                list = new LinkedList<String>();
                groupedItems.put(word, list);
            }
            list.add(file);
        }
        final long groupPhaseDuration = System.currentTimeMillis() - groupPhaseStartTime;
        
        // Print time taken for group phase
		System.out.println("Time taken for grouping phase : " + groupPhaseDuration + "ms");
        pW.println("Time taken for grouping phase : " + groupPhaseDuration + "ms");
		
        // REDUCE:
        final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
            @Override
            public synchronized void reduceDone(String k, Map<String, Integer> v) {
                output.put(k, v);
            }
        };
        final long reduceStartTime = System.currentTimeMillis();	
        executor = Executors.newFixedThreadPool(threadPoolSize);
        
        Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
        while(groupedIter.hasNext()) {
            Map.Entry<String, List<String>> entry = groupedIter.next();
            final String word = entry.getKey();
            final List<String> list = entry.getValue();

            executor.execute(() -> reduce(word, list, reduceCallback));
        }

        executor.shutdown();
        while(!executor.isTerminated());

        final long reduceDuration = System.currentTimeMillis() - reduceStartTime;
		
        System.out.println("Time taken for threadpool to be set up and reduce phase to execute: " + reduceDuration + "ms");
        pW.println("Time taken for threadpool to be set up and reduce phase to execute: " + reduceDuration + "ms");
        
        System.out.println(output);
        pW.println(output);
        
        final long programDuration = System.currentTimeMillis() - programStartTime;
		System.out.println("Time taken entire program to execute: " + programDuration + "ms");
		pW.println("Time taken entire program to execute: " + programDuration + "ms");
		pW.flush();
		pW.close();
    }

    public interface MapCallback<E, V> {
        void mapDone(E key, List<V> values);
    }

    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<>(words.length);
        char initChar;
        String initLetter;

        for(String word: words) {
        	initChar = word.charAt(0);
        	if(Character.isLetter(initChar)){
        		initLetter = String.valueOf(Character.toUpperCase(initChar));
        		if(initLetter.matches("[A-Z]+")){
        			results.add(new MappedItem(initLetter, file));
        		}	
        	}
        }
        callback.mapDone(file, results);
    }

    public interface ReduceCallback<E, K, V> {
        void reduceDone(E e, Map<K, V> results);
    }

    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new ConcurrentHashMap<String,Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }
        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }
}