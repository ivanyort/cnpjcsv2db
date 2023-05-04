/*
by Ivan Yort (ivanyort@gmail.com)
 */
package br.com.yort.cnpjcsv2db;

import br.com.yort.downloader.Downloader;
import br.com.yort.downloader.MultiThreadedDownloader;
import br.com.yort.downloader.MultiThreadedUnzip;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;

public class Main {

    public static String dirZip = "zip";
    public static String dirCsv = "csv";
    public static String receitaUrl = "http://200.152.38.155/CNPJ/";

    public static void main(String[] args) throws Exception {
        cleanup();

        //download();
        unzip();
    }

    public static void download() throws IOException {
        File fileCsv = new File(dirZip);

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

        String html = Downloader.readStringFromURL(receitaUrl);
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
        MultiThreadedUnzip.run(dirZip, dirCsv);
    }

    private static void cleanup() throws IOException {
        clean(dirCsv);
        //clean(dirZip);
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

}
