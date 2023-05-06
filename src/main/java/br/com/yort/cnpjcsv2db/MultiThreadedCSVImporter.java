package br.com.yort.cnpjcsv2db;

import static br.com.yort.cnpjcsv2db.Main.newConnection;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class MultiThreadedCSVImporter {

    public static void run(String csvDirectoryPath) {
        System.out.println("Importando arquivos de " + new File(csvDirectoryPath).getAbsolutePath());
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        File[] csvFiles = new File(csvDirectoryPath).listFiles((dir, name) -> name.toLowerCase().contains("k"));
        for (File csvFile : csvFiles) {
            executor.execute(() -> {
                importCSV(csvFile, tabela(csvFile));
            });
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException ex) {
        }

    }

    public static void importCSV(File file, String tabela) {
        if (tabela == null) {
            return;
        }
        try {
            if (file.getName().equals("kESTABELE.CSV")) {
                int a = 0;
            }
            file = transform(tabela, file);
        } catch (FileNotFoundException | UnsupportedEncodingException ex) {
            Logger.getLogger(MultiThreadedCSVImporter.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(MultiThreadedCSVImporter.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        Connection conn = newConnection();
        if (conn == null) {
            return;
        }
        try {
            CopyManager copyManager = new CopyManager((BaseConnection) conn);
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file), "WINDOWS-1252"));
            copyManager.copyIn("COPY " + tabela + " FROM STDOUT DELIMITER ';' CSV", br);
            br.close();
            if (1 == 2) {
                Files.deleteIfExists(file.toPath());
            }
            conn.close();
        } catch (SQLException | IOException ex) {
            System.out.println(file.getAbsolutePath());
            Logger.getLogger(MultiThreadedCSVImporter.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                conn.close();
            } catch (SQLException ex) {
            }
        }
    }

    private static String tabela(File csvFile) {
        String tabela = null;
        if (csvFile.getName().toLowerCase().contains("cnae")) {
            tabela = "cnae";
        }
        if (csvFile.getName().toLowerCase().contains("emprecsv")) {
            tabela = "empresas";
        }
        if (csvFile.getName().toLowerCase().contains("estabele")) {
            tabela = "estabelecimento";
        }
        if (csvFile.getName().toLowerCase().contains("moticsv")) {
            tabela = "motivo";
        }
        if (csvFile.getName().toLowerCase().contains("municcsv")) {
            tabela = "municipio";
        }
        if (csvFile.getName().toLowerCase().contains("natjucsv")) {
            tabela = "natureza_juridica";
        }
        if (csvFile.getName().toLowerCase().contains("paiscsv")) {
            tabela = "pais";
        }
        if (csvFile.getName().toLowerCase().contains("qualscsv")) {
            tabela = "qualificacao_socio";
        }
        if (csvFile.getName().toLowerCase().contains("simples")) {
            tabela = "simples";
        }
        if (csvFile.getName().toLowerCase().contains("sociocsv")) {
            tabela = "socios_original";
        }
        if (tabela == null) {
            System.out.println(csvFile.getName());
        }
        return tabela;
    }

    private static File transform(String tabela, File file) throws FileNotFoundException, UnsupportedEncodingException, IOException {
        if (!tabela.equals("estabelecimento")) {
            return file;
        }
        File filetmp = new File(file.getAbsolutePath().replaceAll("ESTABELE", "") + ".POSTGRESQL_CSV");
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "WINDOWS-1252"));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filetmp), "WINDOWS-1252"));
        final CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter(';'));
        CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.POSTGRESQL_CSV.withDelimiter(';'));
        try {
            for (final CSVRecord record : parser) {
                for (int i = 0; i < record.size(); i++) {
                    String s = filter(record.get(i));
                    csvPrinter.print(s);
                }
                csvPrinter.println();
            }
        } finally {
            parser.close();
            reader.close();
            csvPrinter.close();
            writer.close();
        }
        return filetmp;
    }

    public static String filter(String str) {
        if (str == null) {
            return null;
        }
        StringBuilder filtered = new StringBuilder(str.length());
        for (int i = 0; i < str.length(); i++) {
            char current = str.charAt(i);
            if (current >= 0x20 && current <= 0x7e) {
                filtered.append(current);
            }
        }
        return filtered.toString().trim();
    }
}
