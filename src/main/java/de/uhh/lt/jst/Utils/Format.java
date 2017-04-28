package de.uhh.lt.jst.Utils;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.zip.GZIPInputStream;


public class Format {

    private static String NOT_SEP = ";";

    public static void ensureDir(String directoryPath){
        File dir = new File(directoryPath);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    private static String trimScore(String score) {
        String[] parts = score.split("[:#]");
        return parts[0];
    }

    public static String flatten(List<String> list, int maxLength, boolean keepScores, String sep) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String str : list) {
            if (i > maxLength) break;

            str = str.replace(",", NOT_SEP);
            str = str.replace(sep, NOT_SEP);
            if (keepScores) sb.append(str);
            else sb.append(trimScore(str));
            sb.append(sep);
            i++;
        }
        return sb.toString().trim();
    }

    private static HashSet<String> _stopDependencies = new HashSet<>(Arrays.asList("root"));

    public static boolean isStopDependencyType(String dtype){
        return _stopDependencies.contains(dtype.toLowerCase());
    }

    public static String join(List<String> list, String sep) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String str : list) {
            i++;
            if (!str.equals("") && !str.contains(sep)) {
                sb.append(str);
                if (i < list.size()) sb.append(sep);
            }
        }
        return sb.toString().trim();
    }

    public static List<String> readAsList(String filePath) throws IOException {
        InputStream fileStream = new FileInputStream(filePath);
        Reader decoder = new InputStreamReader(fileStream, Charset.forName("UTF-8"));
        BufferedReader br = new BufferedReader(decoder);

        String line;
        List<String> res = new LinkedList<>();
        while ((line = br.readLine()) != null) {
            res.add(line);
        }
        return res;
    }

    public static List<String> readGzipAsList(String gzipPath) throws IOException {
        InputStream fileStream = new FileInputStream(gzipPath);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, Charset.forName("UTF-8"));
        BufferedReader br = new BufferedReader(decoder);

        String line;
        List<String> res = new LinkedList<>();
        while ((line = br.readLine()) != null) {
            res.add(line);
        }
        return res;
    }
}
