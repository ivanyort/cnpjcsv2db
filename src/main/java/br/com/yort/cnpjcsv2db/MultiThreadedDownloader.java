package br.com.yort.cnpjcsv2db;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.time.Instant;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

public class MultiThreadedDownloader {

    private static Long bytesDownloaded = 0L;
    private static long startTimeMillis = 0L;
    private static long lastTimeMillis = 0L;
    private static Long totalBytes = 0L;
    Timer timer = new Timer();
    private Progresso progresso = new Progresso();

    public MultiThreadedDownloader() {
        bytesDownloaded = 0L;
        startTimeMillis = 0L;
        lastTimeMillis = 0L;
        totalBytes = 0L;
        timer.schedule(progresso, 0, 100);
    }

    public static String repetirCaractere(char caractere, int quantidade) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < quantidade; i++) {
            sb.append(caractere);
        }
        return sb.toString();
    }

    public boolean multiDownload(List<String> urls, String dirCsv) throws MalformedURLException, IOException {
        totalBytes = 0L;
        for (int i = 0; i < urls.size(); i++) {
            String urltxt = urls.get(i);
            URL url = new URL(urltxt);
            URLConnection conn = url.openConnection();
            int contentLength = conn.getContentLength();
            totalBytes += contentLength;
            System.out.print(String.format("\033[K\r%-50s %d bytes", urltxt, contentLength));
        }
        Double speed = totalBytes.doubleValue();
        String s = "bytes";
        if (speed >= 1024) {
            speed = speed / 1024;
            s = "kb";
        }
        if (speed >= 1024) {
            speed = speed / 1024;
            s = "mb";
        }
        if (speed >= 1024) {
            speed = speed / 1024;
            s = "gb";
        }
        System.out.println(String.format("\033[K\rDonwload Total = %.1f %s" + "\033[K", speed, s, totalBytes));

        lastTimeMillis = 0L;
        startTimeMillis = Instant.now().toEpochMilli();
        int threadCount = 5; //Runtime.getRuntime().availableProcessors();
        Thread[] threads = new Thread[threadCount];

        // criar um objeto de download para cada thread
        for (int i = 0; i < threadCount; i++) {
            DownloadThread downloadThread = new DownloadThread(urls, i, threadCount, dirCsv);
            threads[i] = new Thread(downloadThread);
        }

        // iniciar cada thread
        for (Thread thread : threads) {
            thread.start();
        }

        // esperar por cada thread terminar        
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                System.err.println("Erro ao esperar por thread: " + e.getMessage());
                timer.cancel();
                return false;
            }
        }
        timer.cancel();
        bytesDownloaded = totalBytes;
        progresso.run();
        System.out.println();
        return true;
    }

    private static class DownloadThread implements Runnable {

        private final List<String> urls;
        private final int startIndex;
        private final int step;
        private final String dirCsv;

        public DownloadThread(List<String> urls, int startIndex, int step, String dirCsv) {
            this.urls = urls;
            this.startIndex = startIndex;
            this.step = step;
            this.dirCsv = dirCsv;
        }

        @Override
        public void run() {
            for (int i = startIndex; i < urls.size(); i += step) {
                String url = urls.get(i);
                try (BufferedInputStream in = new BufferedInputStream(new URL(url).openStream()); FileOutputStream fileOutputStream = new FileOutputStream(dirCsv + File.separator + getFileName(url))) {
                    byte[] dataBuffer = new byte[16384];
                    int bytesRead;
                    while ((bytesRead = in.read(dataBuffer, 0, 16384)) != -1) {
                        synchronized (bytesDownloaded) {
                            bytesDownloaded += bytesRead;
                        }
                        fileOutputStream.write(dataBuffer, 0, bytesRead);
                    }
                } catch (IOException e) {
                    System.err.println("Erro ao fazer download de " + url + ": " + e.getMessage());
                }
            }
        }

        private String getFileName(String url) {
            int index = url.lastIndexOf("/");
            return url.substring(index + 1);
        }

    }

    public class Progresso extends TimerTask {

        @Override
        public void run() {
            if (bytesDownloaded == 0) {
                return;
            }
            lastTimeMillis = Instant.now().toEpochMilli();
            long elapsed = (lastTimeMillis - startTimeMillis) / 1000;
            if (elapsed != 0) {
                String s = "b/s";
                String s1 = "bytes";
                double speed = bytesDownloaded / elapsed; // bytes por segundo
                double total = bytesDownloaded;
                if (speed >= 1024) {
                    speed = speed / 1024;
                    s = "kb/s";
                }
                if (speed >= 1024) {
                    speed = speed / 1024;
                    s = "mb/s";
                }
                if (speed >= 1024) {
                    speed = speed / 1024;
                    s = "gb/s";
                }
                if (total >= 1024) {
                    total = total / 1024;
                    s1 = "kb";
                }
                if (total >= 1024) {
                    total = total / 1024;
                    s1 = "mb";
                }
                if (total >= 1024) {
                    total = total / 1024;
                    s1 = "gb";
                }

                double percent = ((double) bytesDownloaded * 100) / (double) totalBytes;
                String repetirCaractere = repetirCaractere('█', (int) percent / 2);
                String barra = "[" + repetirCaractere + repetirCaractere('-', (50 - repetirCaractere.length())) + "]";
                System.out.print("\033[K\r" + barra + " " + String.format("%,.1f", percent) + "%" + String.format("  %,.1f %s (restante = %d)" + "\033[K", speed, s, totalBytes - bytesDownloaded));
            }
        }
    }

}
