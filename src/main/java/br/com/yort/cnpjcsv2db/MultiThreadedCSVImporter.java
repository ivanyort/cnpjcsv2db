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
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.StringUtils;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class MultiThreadedCSVImporter {

    static Map<String, Integer> numFields = new LinkedHashMap<>();

    public static void run(String csvDirectoryPath) {

        // Para conferencia de mudanca em layout
        numFields.put("estabelecimento", 30);
        numFields.put("cnae", 2);
        numFields.put("empresas", 7);
        numFields.put("motivo", 2);
        numFields.put("municipio", 2);
        numFields.put("natureza_juridica", 2);
        numFields.put("pais", 2);
        numFields.put("qualificacao_socio", 2);
        numFields.put("simples", 7);
        numFields.put("socios", 11);

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
            file = transform(tabela, file);
        } catch (FileNotFoundException | UnsupportedEncodingException ex) {
            Logger.getLogger(MultiThreadedCSVImporter.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (IOException ex) {
            Logger.getLogger(MultiThreadedCSVImporter.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (SQLException ex) {
            Logger.getLogger(MultiThreadedCSVImporter.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        if (file == null) {
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
            tabela = "socios";
        }
        if (tabela == null) {
            System.out.println(csvFile.getName());
        }
        return tabela;
    }

    private static File transform(String tabela, File file) throws FileNotFoundException, UnsupportedEncodingException, SQLException {
        Connection conn = newConnection();
        DatabaseMetaData metaData = conn.getMetaData();
        Map<String, String> fields = new LinkedHashMap<>();
        try (ResultSet rs = metaData.getColumns(null, null, tabela, null)) {
            while (rs.next()) {
                fields.put(rs.getString("COLUMN_NAME"), rs.getString("TYPE_NAME"));
            }
            rs.close();
        }
        conn.close();

        String tempName = file.getParent() + File.separator + UUID.randomUUID().toString().replaceAll("-", "") + ".POSTGRESQL_CSV";
        File filetmp = new File(tempName);
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), "WINDOWS-1252"));
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filetmp), "WINDOWS-1252"));
        CSVParser parser = null;
        try {
            parser = new CSVParser(reader, CSVFormat.DEFAULT.withDelimiter(';'));
        } catch (IOException ex) {
            Logger.getLogger(MultiThreadedCSVImporter.class.getName()).log(Level.SEVERE, null, ex);
        }
        CSVPrinter csvPrinter = null;
        try {
            csvPrinter = new CSVPrinter(writer, CSVFormat.POSTGRESQL_CSV.withDelimiter(';').withQuoteMode(QuoteMode.ALL_NON_NULL));
        } catch (IOException ex) {
            Logger.getLogger(MultiThreadedCSVImporter.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            for (final CSVRecord record : parser) {
                if (record.size() != numFields.get(tabela)) {
                    System.out.println("Possivel alteracao na tabela " + tabela + " nao foi importada");
                    return null;
                }
                for (int i = 0; i < record.size(); i++) {
                    Object fieldName = fields.keySet().toArray()[i];
                    Object fieldType = fields.get(fieldName);
                    csvPrinter.print(filter(record.get(i), tabela, fieldName, fieldType));
                }
                // campos adicionais calculados
                if (tabela.equals("estabelecimento")) {
                    //cnpj
                    String f0 = record.get(0);
                    String f1 = record.get(1);
                    String f2 = record.get(2);
                    f0 = f0.substring(0, 2) + "." + f0.substring(2, 5) + "." + f0.substring(5, 8);
                    csvPrinter.print(f0 + "/" + f1 + "-" + f2);
                }
                csvPrinter.println();
            }
        } catch (Exception ex) {
            Logger.getLogger(MultiThreadedCSVImporter.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                parser.close();
                reader.close();
                csvPrinter.close();
                writer.close();
            } catch (IOException ex) {
                Logger.getLogger(MultiThreadedCSVImporter.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return filetmp;
    }

    public static boolean isDateValid(String date) {
        try {
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
            df.setLenient(false);
            df.parse(date);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    public static String filter(String str, String tabela, Object fieldName, Object fieldType) {
        if (str == null) {
            return null;
        }
        if (str.trim().length() == 0) {
            return null;
        }
        if (fieldName.equals("correio_eletronico")) {
            str = str.toLowerCase().trim();
        }
        if (fieldType.equals("date")) {
            if (str.length() >= 8) {
                str = str.substring(0, 4) + "-" + str.substring(4, 6) + "-" + str.substring(6, 8);
            }
            if (!isDateValid(str)) {
                return null;
            }
        } else if (fieldType.equals("varchar")) {

        } else {
            str = StringUtils.replace(str, ".", "");
            str = StringUtils.replace(str, ",", ".");
        }
        StringBuilder filtered = new StringBuilder(str.length());
        for (int i = 0; i < str.length(); i++) {
            char current = str.charAt(i);
            if (current >= 0x20 && current <= 0x7e) {
                filtered.append(current);
            }
        }
        String ret = filtered.toString().trim();
        while (ret.contains("  ")) {
            ret = StringUtils.replace(ret, "  ", " ");
        }
        return ret;
    }
}
