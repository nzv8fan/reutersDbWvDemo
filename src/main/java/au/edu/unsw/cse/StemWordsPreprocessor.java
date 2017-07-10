package au.edu.unsw.cse;

import org.tartarus.snowball.ext.EnglishStemmer;

/**
 * Created by bradfordh on 10/07/17.
 */
public class StemWordsPreprocessor extends StripSpecialCharactersPreprocessor {

    /**
     * The stemmer object used to stem words
     */
    private EnglishStemmer stemmer;

    public StemWordsPreprocessor() {
        stemmer = new EnglishStemmer();
    }

    /**
     * This needs to be synchronized because it is run in a mutli-threaded environment.
     * @param token
     * @return
     */
    public String preProcess(String token) {
        String words = super.preProcess(token);

        synchronized(this) {
            stemmer.setCurrent(words);
            try {
                stemmer.stem();
            } catch (java.lang.StringIndexOutOfBoundsException e) {
                System.out.println(words);
                throw e;
            }

            return stemmer.getCurrent();
        }
    }

}
