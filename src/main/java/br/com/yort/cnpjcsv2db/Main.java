/*
by Ivan Yort (ivanyort@gmail.com)
 */
package br.com.yort.cnpjcsv2db;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.fusesource.jansi.AnsiConsole;
import static org.fusesource.jansi.Ansi.*;
import static org.fusesource.jansi.Ansi.Color.*;

public class Main {

    public static String dirZip = "zip";
    public static String dirCsv = "csv";
    public static String receitaUrl = "http://200.152.38.155/CNPJ/";
    public static String dbHost = "postgres";
    public static String dbUser = "postgres";
    public static String dbPass = "Qlik123$";
    public static String dbSsl = "false";
    public static String dbName = "rfb";
    public static String dbPort = "5432";
    public static String dbSchema = "cnpj";
    public static StopWatch stopWatch = new StopWatch();

    public static void main(String[] args) throws Exception {
        AnsiConsole.systemInstall();
        System.out.println(ansi().fg(WHITE).bg(BLACK));
        stopWatch.start();
        createDb();
        System.out.println(ansi().fg(WHITE).a(split()));        
        cleanup();
        System.out.println(ansi().fg(WHITE).a(split()));
        download();
        System.out.println(ansi().fg(WHITE).a(split()));
        unzip();
        System.out.println(ansi().fg(WHITE).a(split()));         
        dbImport();
        System.out.println(ansi().fg(WHITE).a(split()));
        System.out.println(ansi().fg(WHITE).a(stopWatch.getTime(TimeUnit.MINUTES)));
        AnsiConsole.systemUninstall();
    }

    private static void download() throws IOException {
        File fileCsv = new File(dirZip);
        System.out.println(ansi().fg(WHITE).a("Download de dados para " + fileCsv.getAbsolutePath() + " ...").fg(BLUE));

        if (fileCsv.exists()) {
            if (!fileCsv.isDirectory()) {
                System.err.println("Verifique existencia de " + fileCsv.getAbsolutePath());
                System.exit(-1);
            }
        } else {
            if (!fileCsv.mkdirs()) {
                System.err.println("Impossivel criar " + fileCsv.getAbsolutePath());
                System.exit(-1);
            }
        }

        String html = readStringFromURL(receitaUrl);
        Pattern pattern = Pattern.compile("href=\"(.+?\\.zip)\"");
        Matcher matcher = pattern.matcher(html);
        List<String> urls = new ArrayList<>();
        while (matcher.find()) {
            urls.add(receitaUrl + matcher.group(1));
        }
        if (urls.isEmpty()) {
            System.err.println("Nao foi possivel obter arquivos para download a partir de " + receitaUrl);
            System.exit(-1);
        }

        MultiThreadedDownloader mlt = new MultiThreadedDownloader();

        mlt.multiDownload(urls, dirZip);
    }

    private static void unzip() throws IOException {
        System.out.println(ansi().fg(WHITE).a("Descompactando dados para " + new File(dirCsv).getAbsolutePath() + " ...").fg(BLUE));
        MultiThreadedUnzip.run(dirZip, dirCsv);
    }

    private static void cleanup() throws IOException {
        System.out.println(ansi().fg(WHITE).a("Limpeza downloads anteriores...").fg(BLUE));
        clean(dirCsv);
        clean(dirZip);
    }

    private static void clean(String dir) throws IOException {
        File directory = new File(dir);

        //verifica se o diretório existe
        if (!directory.exists()) {
            directory.mkdirs();
        }

        //verifica se o objeto File representa um diretório
        if (!directory.isDirectory()) {
            System.out.println(directory.getAbsolutePath() + " não é um diretório.");
            return;
        }

        //apaga todos os arquivos do diretório
        FileUtils.cleanDirectory(directory);

        //apaga o diretório em si
        FileUtils.deleteDirectory(directory);

        directory.mkdirs();
    }

    private static void dbImport() {
        System.out.println(ansi().fg(WHITE).a("Importando para o banco de dados...").fg(BLUE));
        MultiThreadedCSVImporter.run(dirCsv);
    }

    public static String readStringFromURL(String requestURL) throws IOException {
        try (Scanner scanner = new Scanner(new URL(requestURL).openStream(),
                StandardCharsets.UTF_8.toString())) {
            scanner.useDelimiter("\\A");
            return scanner.hasNext() ? scanner.next() : "";
        }
    }

    private static void createDb() throws SQLException {
        System.out.println(ansi().fg(WHITE).a("Criando banco de dados se necessário...").fg(BLUE));
        Connection conn = newConnection();
        List<String> sqlStatements = new ArrayList<>();
        sqlStatements.add("DROP TABLE if exists cnae");
        sqlStatements.add("CREATE TABLE cnae (codigo VARCHAR(7),descricao VARCHAR(200))");
        sqlStatements.add("DROP TABLE if exists empresas");
        sqlStatements.add("CREATE TABLE empresas (cnpj_basico VARCHAR(8),razao_social VARCHAR(200),natureza_juridica VARCHAR(4),qualificacao_responsavel VARCHAR(2),capital_social_str VARCHAR(20),porte_empresa VARCHAR(2),ente_federativo_responsavel VARCHAR(50))");
        sqlStatements.add("DROP TABLE if exists estabelecimento");
        sqlStatements.add("CREATE TABLE estabelecimento (cnpj_basico VARCHAR(8),cnpj_ordem VARCHAR(4),cnpj_dv VARCHAR(2),matriz_filial VARCHAR(1),nome_fantasia VARCHAR(200),situacao_cadastral VARCHAR(2),data_situacao_cadastral VARCHAR(8),motivo_situacao_cadastral VARCHAR(2),nome_cidade_exterior VARCHAR(200),pais VARCHAR(3),data_inicio_atividades VARCHAR(8),cnae_fiscal VARCHAR(7),cnae_fiscal_secundaria VARCHAR(1000),tipo_logradouro VARCHAR(20),logradouro VARCHAR(200),numero VARCHAR(10),complemento VARCHAR(200),bairro VARCHAR(200),cep VARCHAR(8),uf VARCHAR(2),municipio VARCHAR(4),ddd1 VARCHAR(4),telefone1 VARCHAR(8),ddd2 VARCHAR(4),telefone2 VARCHAR(8),ddd_fax VARCHAR(4),fax VARCHAR(8),correio_eletronico VARCHAR(200),situacao_especial VARCHAR(200),data_situacao_especial VARCHAR(8))");
        sqlStatements.add("DROP TABLE if exists motivo");
        sqlStatements.add("CREATE TABLE motivo (codigo VARCHAR(2),descricao VARCHAR(200))");
        sqlStatements.add("DROP TABLE if exists municipio");
        sqlStatements.add("CREATE TABLE municipio (codigo VARCHAR(4),descricao VARCHAR(200))");
        sqlStatements.add("DROP TABLE if exists natureza_juridica");
        sqlStatements.add("CREATE TABLE natureza_juridica (codigo VARCHAR(4),descricao VARCHAR(200))");
        sqlStatements.add("DROP TABLE if exists pais");
        sqlStatements.add("CREATE TABLE pais (codigo VARCHAR(3),descricao VARCHAR(200))");
        sqlStatements.add("DROP TABLE if exists qualificacao_socio");
        sqlStatements.add("CREATE TABLE qualificacao_socio (codigo VARCHAR(2),descricao VARCHAR(200))");
        sqlStatements.add("DROP TABLE if exists simples");
        sqlStatements.add("CREATE TABLE simples (cnpj_basico VARCHAR(8),opcao_simples VARCHAR(1),data_opcao_simples VARCHAR(8),data_exclusao_simples VARCHAR(8),opcao_mei VARCHAR(1),data_opcao_mei VARCHAR(8),data_exclusao_mei VARCHAR(8))");
        sqlStatements.add("DROP TABLE if exists socios_original");
        sqlStatements.add("CREATE TABLE socios_original (cnpj_basico VARCHAR(8),identificador_de_socio VARCHAR(1),nome_socio VARCHAR(200),cnpj_cpf_socio VARCHAR(14),qualificacao_socio VARCHAR(2),data_entrada_sociedade VARCHAR(8),pais VARCHAR(3),representante_legal VARCHAR(11),nome_representante VARCHAR(200),qualificacao_representante_legal VARCHAR(2),faixa_etaria VARCHAR(1))");

        Statement stmt = conn.createStatement();
        for (String sqlStatement : sqlStatements) {
            stmt.execute(sqlStatement);
        }
        stmt.close();
        conn.close();
    }

    public static Connection newConnection() {
        Connection conn = null;
        while (true) {
            String dbConnectonUrl = "jdbc:postgresql://" + dbHost + ":" + dbPort + "/" + dbName + "?cancelSignalTimeout=30000&tcpKeepAlive=true&ssl=false&sslmode=allow&currentSchema=" + dbSchema;
            Properties props = new Properties();
            props.setProperty("user", dbUser);
            props.setProperty("password", dbPass);
            props.setProperty("ssl", dbSsl);
            try {
                conn = DriverManager.getConnection(dbConnectonUrl, props);
                Statement stmt = conn.createStatement();
                stmt.execute("create schema if not exists " + dbSchema);
                stmt.close();
                break;
            } catch (SQLException ex) {
                if (ex.getMessage().contains("does not exist") && ex.getMessage().contains("database")) {
                    try {
                        dbConnectonUrl = "jdbc:postgresql://" + dbHost + ":" + dbPort + "/postgres";
                        conn = DriverManager.getConnection(dbConnectonUrl, props);
                        Statement stmt = conn.createStatement();
                        stmt.execute("create database " + dbName);
                        stmt.close();
                        conn.close();
                        continue;
                    } catch (SQLException ex1) {
                        Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex1);
                        break;
                    }
                }
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
                break;
            }
        }
        return conn;
    }

    private static String split() {
        stopWatch.split();
        return stopWatch.toSplitString();
    }
}
