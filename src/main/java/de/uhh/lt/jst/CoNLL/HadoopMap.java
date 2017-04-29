package de.uhh.lt.jst.CoNLL;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.trees.TypedDependency;
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
    String outputFormat;

    @Override
    public void setup(Context context) throws IOException {
        outputFormat = context.getConfiguration().getStrings("outputFormat", "list")[0]; // or "conll"
        log.info("Output format: " + outputFormat);

        Properties props = new Properties();
        props.put("language", "english");
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, ner, depparse");
        props.put("depparse.model", "edu/stanford/nlp/models/parser/nndep/english_SD.gz");
        props.put("parse.originalDependencies", true);
        pipeline = new StanfordCoreNLP(props);
    }

    @Override
    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        try {
            Annotation document = new Annotation(line.toString());
            pipeline.annotate(document);
            List<CoreMap> coreMapSentences = document.get(SentencesAnnotation.class);
            for (CoreMap sentence: coreMapSentences){
                context.getCounter("de.uhh.lt.jst", "NUM_SENTENCES").increment(1);
                SemanticGraph semanticGraph = sentence.get(BasicDependenciesAnnotation.class);

                String sentenceStr = sentence.toString().replace("\t", " ");
                if (outputFormat.equals("conll")){
                    // the conll format

                    // For each dependency output a field with ten columns ending with the bio named entity: http://universaldependencies.org/docs/format.html
                    // IN_ID TOKEN LEMMA POS_COARSE POS_FULL MORPH ID_OUT TYPE _ NE_BIO
                    // An example is below. NB: the last (10th) field can be anything according to the specification (NE in our case)
                    // 5 books book NOUN NNS Number=Plur 2 dobj 4:dobj SpaceAfter=No

                    context.write(new Text("-1\t" + sentenceStr), NullWritable.get());
                    TreeMap<Integer, String> conllLines = new TreeMap<>();
                    for (TypedDependency td : semanticGraph.typedDependencies()){
                        Integer idSrc = td.dep().index();
                        String depType = td.reln().getShortName();
                        String token = td.dep().originalText();
                        String lemma = td.dep().lemma();
                        String pos = td.dep().tag();
                        String posFull = td.dep().tag();
                        Integer idDst = td.gov().index();
                        String ner = td.dep().ner();
                        String conllLine = String.format("%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s",
                            token,
                            lemma,
                            pos,
                            posFull,
                            "_",
                            idDst,
                            depType,
                            "_",
                            ner);
                        conllLines.put(idSrc, conllLine);
                    }

                    for (Integer id : conllLines.keySet()) {
                        String res = String.format("%d\t%s", id, conllLines.get(id));
                        context.getCounter("de.uhh.lt.jst", "NUM_DEPS").increment(1);
                        context.write(new Text(res), NullWritable.get());
                    }

                } else {
                    // the default simpler "list" format
                    String parseStr = semanticGraph.typedDependencies().toString();
                    context.write(new Text(sentenceStr + "\t" + parseStr), NullWritable.get());
                }
            }
        } catch(Exception e){
            log.error("Can't process line: " + line.toString(), e);
            context.getCounter("de.uhh.lt.jst", "NUM_MAP_ERRORS").increment(1);
        }
    }
}

