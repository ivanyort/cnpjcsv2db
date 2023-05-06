package br.com.yort.cnpjcsv2db;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class MultiThreadedUnzip {

    public static void run(String zipDirectoryPath,String outputDirectoryPath) {
        //cria um pool de threads com tamanho fixo
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        //lista todos os arquivos zip no diretório
        File[] zipFiles = new File(zipDirectoryPath).listFiles((dir, name) -> name.toLowerCase().endsWith(".zip"));

        //percorre todos os arquivos zip e adiciona uma tarefa para descompactá-los ao pool de threads
        for (File zipFile : zipFiles) {
            executor.execute(() -> {
                try {
                    unzipFile(zipFile, outputDirectoryPath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }

        //finaliza o pool de threads quando todas as tarefas forem concluídas
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException ex) {            
        }
    }

    private static void unzipFile(File zipFile, String outputDirectoryPath) throws IOException {
        //cria o diretório de destino se ele não existir
        File outputDirectory = new File(outputDirectoryPath);
        if (!outputDirectory.exists()) {
            outputDirectory.mkdirs();
        }

        //cria um stream de entrada a partir do arquivo zip
        try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFile))) {
            //percorre todas as entradas do arquivo zip
            ZipEntry zipEntry = zipInputStream.getNextEntry();
            while (zipEntry != null) {
                //cria o arquivo de saída
                File outputFile = new File(outputDirectoryPath + File.separator + zipEntry.getName());
                if (zipEntry.isDirectory()) {
                    //cria um diretório de saída
                    outputFile.mkdirs();
                } else {
                    //cria um arquivo de saída e copia o conteúdo do arquivo zip para ele
                    try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
                        byte[] buffer = new byte[8192];
                        int bytesRead;
                        while ((bytesRead = zipInputStream.read(buffer)) != -1) {
                            outputStream.write(buffer, 0, bytesRead);
                        }
                    }
                }
                //avança para a próxima entrada do arquivo zip
                zipEntry = zipInputStream.getNextEntry();
            }
        }
    }
}
