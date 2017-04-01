package MapReduce;

import java.io.*;
import java.util.Scanner;

public class ReadTextFile {

	
	static String[] readFile(File file) throws IOException {

        StringBuilder fileContents = new StringBuilder((int)file.length());
        String filename = file.getName();
        Scanner scanner = new Scanner(file);
        String lineSeparator = System.getProperty("line.separator");

        try {
            while(scanner.hasNextLine()) {
                fileContents.append(scanner.nextLine() + lineSeparator);
            }
        } finally {
            scanner.close();
        }
        return new String[]{ filename, fileContents.toString() };

    }
} 
