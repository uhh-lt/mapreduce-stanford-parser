package de.uhh.lt.jst.CoNLL;

import de.uhh.lt.jst.TestPaths;
import de.uhh.lt.jst.Utils.Format;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.*;


public class HadoopTest {
    private void run(Configuration conf, long expectedLines) throws Exception {
        TestPaths paths = new TestPaths("standard");
        FileUtils.deleteDirectory(new File(paths.getOutputDir()));
        ToolRunner.run(conf, new HadoopMain(), new String[]{paths.getInputPath(), paths.getOutputDir(), "false"});

        String outputPath = (new File(paths.getOutputDir(), "part-m-00000")).getAbsolutePath();
        List<String> lines = Format.readAsList(outputPath);
        assertEquals("Number of lines in the output file is wrong.", expectedLines, lines.size());
    }

    @Test
    public void testDefaultConfiguration() throws Exception {
        run(new Configuration(), 14);
    }
}

