package au.edu.unsw.cse;

import org.apache.uima.resource.ResourceInitializationException;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.word2vec.Word2Vec;
import org.deeplearning4j.text.sentenceiterator.BasicResultSetIterator;
import org.deeplearning4j.text.sentenceiterator.SentenceIterator;
import org.deeplearning4j.text.sentenceiterator.UimaResultSetIterator;
import org.deeplearning4j.text.tokenization.tokenizer.preprocessor.CommonPreprocessor;
import org.deeplearning4j.text.tokenization.tokenizerfactory.DefaultTokenizerFactory;
import org.deeplearning4j.text.tokenization.tokenizerfactory.TokenizerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Bradford Heap on 19/10/16.
 * Updated for public release 05/07/17.
 */
public class BuildWordVectorsFromDatabase {

    private static Logger log = LoggerFactory.getLogger(BuildWordVectorsFromDatabase.class);

    public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, ResourceInitializationException {

        if (args.length < 1) {
            System.err.println("USAGE: java BuildWordVectorsFromDatabase 'jdbc:postgresql://server.domain:port/database?user=username&password=password' " +
                    "<iterations> <layer size> <output filename> <sql query> <sql column name> <jdbcClassName>");
            return;
        }
        String serverUrl = args[0];

        int iterations = 1;
        if (args.length > 1) {
            iterations = new Integer(args[1]);
        }

        int layerSize = 100;
        if (args.length > 2) {
            layerSize = new Integer(args[2]);
        }

        String filename = "reutersWord2VecModel.zip";
        if (args.length > 3) {
            filename = args[3];
        }

        String sql = "select distinct(body) as raw_text from reuters_sgm where body not like 'Shr%' and body not like 'Qtly%'";
        if (args.length > 4) {
            sql = args[4];
        }

        String columnName = "raw_text";
        if (args.length > 5) {
            columnName = args[5];
        }

        log.info("Opening Database Connection");

        String jdbcDriverClassName = "org.postgresql.Driver";
        if (args.length > 6) {
            jdbcDriverClassName = args[6];
        }

        // SETUP POSTGRES AND GET DATA
        // Get the jdbc driver
        Class.forName(jdbcDriverClassName);

        // connect to the server.
        java.sql.Connection pconnection = DriverManager.getConnection(serverUrl);

        PreparedStatement pstmt = pconnection.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
        ResultSet row = pstmt.executeQuery();

        log.info("Load & Vectorize Sentences....");

        // Strip white space before and after for each line
//        SentenceIterator iter = new BasicResultSetIterator(row, columnName);
        SentenceIterator iter = new UimaResultSetIterator(row, columnName);
        // Split on white spaces in the line to get words
        TokenizerFactory t = new DefaultTokenizerFactory();
//        t.setTokenPreProcessor(new CommonPreprocessor());
//        t.setTokenPreProcessor(new StripSpecialCharactersPreprocessor());
        t.setTokenPreProcessor(new StemWordsPreprocessor());

        log.info("Building model....");
        Word2Vec vec = new Word2Vec.Builder()
                .minWordFrequency(2)
                .epochs(iterations)
                .layerSize(layerSize)
                .seed(42)
                .windowSize(10)
                .iterate(iter)
                .tokenizerFactory(t)
                .build();

        log.info("Fitting Word2Vec model....");
        vec.fit();

        log.info("Writing word vectors to zip file....");

        log.info("Save vectors....");
        WordVectorSerializer.writeWord2VecModel(vec, filename);

    }
}
