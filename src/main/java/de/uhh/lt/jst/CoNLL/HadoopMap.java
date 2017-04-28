package de.uhh.lt.jst.CoNLL;

import de.uhh.lt.jst.Utils.Format;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.util.CoreMap;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.*;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.semgraph.SemanticGraphCoreAnnotations.BasicDependenciesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;

public class HadoopMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    Logger log = Logger.getLogger("de.uhh.lt.jst");
    StanfordCoreNLP pipeline;

    @Override
    public void setup(Context context) throws IOException {
        Properties props = new Properties();
        props.put("language", "english");
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, depparse");
        props.put("depparse.model", "edu/stanford/nlp/models/parser/nndep/english_SD.gz");
        props.put("parse.originalDependencies", true);
        pipeline = new StanfordCoreNLP(props);
    }

    /*
    public static String getShortName(String dkproType){
        if (dkproType.length() > 0){
            String [] fields = dkproType.split("\\.");
            if (fields.length > 0){
                return fields[fields.length-1];
            } else {
                return dkproType;
            }
        } else {
            return dkproType;
        }
    }

    private String getBIO(List<NamedEntity> ngrams, int beginToken, int endToken){
        for (NamedEntity ngram : ngrams){
            if (ngram.getBegin() == beginToken) {
                return "B-" + getShortName(ngram.getType().getName());
            } else if (ngram.getBegin() < beginToken && ngram.getEnd() >= endToken) {
                return "I-" + getShortName(ngram.getType().getName());
            } else {
                return "O";
            }
        }
        return "O";
    }*/

    @Override
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        try {
            Annotation document = new Annotation(line.toString());
            pipeline.annotate(document);
            List<CoreMap> coreMapSentences = document.get(SentencesAnnotation.class);
            for (CoreMap sentence: coreMapSentences){
                SemanticGraph semanticGraph = sentence.get(BasicDependenciesAnnotation.class);
                String parseStr = semanticGraph.typedDependencies().toString();
                String sentenceStr = sentence.toString().replace("\t", " ");

                context.write(new Text(sentenceStr + "\t" + parseStr), NullWritable.get());
            }

            // For each dependency output a field with ten columns ending with the bio named entity: http://universaldependencies.org/docs/format.html
            // IN_ID TOKEN LEMMA POS_COARSE POS_FULL MORPH ID_OUT TYPE _ NE_BIO
            // An example is below. NB: the last (10th) field can be anything according to the specification (NE in our case)
            // 5 books book NOUN NNS Number=Plur 2 dobj 4:dobj SpaceAfter=No

            /*
            for (Dependency dep : deps) {
                Integer id = tokenToID.getOrDefault(dep.getDependent().getCoveredText(), -2);
                String BIO = getBIO(ngrams, dep.getBegin(), dep.getEnd());
                String conllLine = String.format("%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s",
                        dep.getDependent().getCoveredText(),
                        dep.getDependent().getLemma() != null ? dep.getDependent().getLemma().getValue() : "",
                        dep.getDependent().getPos() != null ? dep.getDependent().getPos().getPosValue() : "",
                        dep.getDependent().getPos() != null ? dep.getDependent().getPos().getPosValue() : "",
                        dep.getDependent().getMorph() != null ? dep.getDependent().getMorph().getValue() : "",
                        tokenToID.getOrDefault(dep.getGovernor().getCoveredText(), -2),
                        dep.getDependencyType(),
                        "_",
                        BIO
                );
                conllLines.put(id, conllLine);
            }

            for (Integer id : conllLines.keySet()) {
                String res = String.format("%d\t%s", id, conllLines.get(id));
                context.write(new Text(res), NullWritable.get());
            }
            */

        } catch(Exception e){
            log.error("Can't process line: " + line.toString(), e);
            context.getCounter("de.uhh.lt.jst", "NUM_MAP_ERRORS").increment(1);
        }
    }
}

