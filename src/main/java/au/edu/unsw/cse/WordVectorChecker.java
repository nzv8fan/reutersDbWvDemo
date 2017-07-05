package au.edu.unsw.cse;

import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Scanner;

/**
 * Created by Bradford Heap on 24/10/16.
 * Updated for public release 05/07/17.
 */
public class WordVectorChecker {

    public static void main(String[] args) throws IOException {

        String word2VecModelFilename = "reutersWord2VecModel.zip";
        if (args.length > 0) {
            word2VecModelFilename = args[0];
        }

        Word2Vec domainSpecificWord2VecModel = WordVectorSerializer.readWord2VecModel(new File(word2VecModelFilename));

        Scanner sc = new Scanner(System.in);
        while (true) {

            System.out.print("Enter a word: ");

            String inputText = sc.nextLine();
            inputText = inputText.toLowerCase().trim();

            System.out.println(inputText);

            System.out.println("Closest Words:");
            Collection<String> lst;
            if (inputText.contains(" ")) {

                String[] words = inputText.split(" ");

                if (words.length == 2) {
                    System.out.println("Distance between words: " + domainSpecificWord2VecModel.similarity(words[0],words[1]));
                }

            } else { // dealing with a single word
                lst = domainSpecificWord2VecModel.wordsNearest(inputText, 10);
                for (String s : lst) {
                    System.out.println(s + " " + domainSpecificWord2VecModel.similarity(inputText, s));
                }
            }

        }
    }

}
