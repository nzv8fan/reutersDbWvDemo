package au.edu.unsw.cse;

import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;

/**
 * Created by Bradford Heap on 5/07/17.
 */
public class StripSpecialCharactersPreprocessor extends CommonPreprocessor {

    public String preProcess(String token) {
        return super.preProcess(token).replaceAll("[^a-zÀ-ÿ_\\-]","");
    }
}
