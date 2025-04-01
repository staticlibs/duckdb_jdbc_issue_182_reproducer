import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Issue182Reproducer {

    static final long CURRENT_PROCESS_PID = ProcessHandle.current().pid();
    static final Pattern VM_RSS_REGEX = Pattern.compile("^VmRSS:\\s+(\\d+)\\s+kB$");
    static final DecimalFormat NUM_FORMAT = new DecimalFormat("###,###,### KB");

    static long readRSSMem() throws IOException {
        Path status = Paths.get("/proc/" + CURRENT_PROCESS_PID + "/status");
        List<String> lines = Files.readAllLines(status, UTF_8);
        for (String ln : lines) {
            if (ln.startsWith("VmRSS")) {
                Matcher matcher = VM_RSS_REGEX.matcher(ln);
                if (matcher.matches()) {
                    return Long.parseLong(matcher.group(1));
                }
            }
        }
        throw new IOException("Error reading VmRSS from /proc/pid/status");
    }

    static long readWMICMem() throws Exception {
        Process process = Runtime.getRuntime()
                .exec("cmd /c wmic process where processid=" + CURRENT_PROCESS_PID + " get WorkingSetSize");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        List<String> lines = new ArrayList<>();
        String ln;
        while ((ln = bufferedReader.readLine()) != null) {
            String trimmed = ln.trim();
            if (trimmed.length() > 0) {
                lines.add(trimmed);
            }
        }
        if (lines.size() != 2) {
            throw new IOException("Error running WMIC, output: " + lines);
        }
        long numBytes = Long.parseLong(lines.get(1));
        return numBytes / 1024;
    }

    static long readOSMem() throws Exception {
        if (System.getProperty("os.name").startsWith("Windows")) {
            return readWMICMem();
        } else {
            return readRSSMem();
        }
    }

    static long readDBMem(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select tag, memory_usage_bytes from duckdb_memory()")) {
            long res = 0;
            while (rs.next()) {
                String tag = rs.getString(1);
                long numBytes = rs.getLong(2);
                res += (numBytes/1024);
            }
            return res;
        }
    }

    static void printMem(Connection conn) throws Exception {
        Runtime runtime = Runtime.getRuntime();
        System.out.println(">>>>>>>");
        System.out.println(" DB: " + NUM_FORMAT.format(readDBMem(conn)));
        System.out.println("JVM: " + NUM_FORMAT.format(runtime.totalMemory()/1024));
        System.out.println(" OS: " + NUM_FORMAT.format(readOSMem()));
        System.out.println("<<<<<<<");
    }

    public static void main(String[] args) throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
            Statement stmt = conn.createStatement()) {
            stmt.execute("SET memory_limit='10GB'");
            stmt.execute("SET threads='1'");
            printMem(conn);
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM read_parquet('../data/*.parquet') order by pickup_at")) {
                printMem(conn);
                int colCount = rs.getMetaData().getColumnCount();
                int count = 0;
                while (rs.next()) {
                    for (int i = 1; i <= colCount; i++) {
                        rs.getString(i);
                    }
                    if (0 == ++count % 1000000) {
                        System.out.println(count);
                        printMem(conn);
                    }
                }
            }
        }
    }
}
