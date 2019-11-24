package org.apache.nifi.processors.standard;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class TestExtractHeader {

    final String NO_LINES = "";
    final String ONE_LINE = "1";
    final String ONE_LINE_NL = "2\n";
    final String ONE_LINE_MAC_NL = "1\r";
    final String ONE_LINE_WIN_NL = "1\r\n";
    final String TWO_LINES = "1\n2";
    final String TWO_LINES_NL = "1\n2\n";
    final String TWO_LINES_MAC_NL = "1\n2\r";
    final String TWO_LINES_WIN_NL = "1\n2\r\n";

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ExtractHeader.class);
    }

    @Test
    public void testOneLineNl() throws Exception {
        testRunner.enqueue(TWO_LINES_NL.getBytes("UTF-8"));
        testRunner.run();

        final MockFlowFile removedFf = testRunner.getFlowFilesForRelationship(ExtractHeader.REMOVED).get(0);
        removedFf.assertContentEquals("1\n");

        final MockFlowFile bodyFf = testRunner.getFlowFilesForRelationship(ExtractHeader.BODY).get(0);
        bodyFf.assertContentEquals("2\n");
    }
}
