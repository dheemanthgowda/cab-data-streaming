package com.learning.flink.datastream.datasource;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;

public class DataServer {
  static final String filePath =
      "/Users/dg072881/Documents/GitHub/cab-data-streaming/src/main/resource/test.txt";

  public static void main(String[] args) throws IOException {
    readDataProcessingTime();
  }

  public static void readDataProcessingTime() throws IOException {
    File file = new File(filePath);
    Scanner sc = new Scanner(file);
    try (ServerSocket listener = new ServerSocket(9090)) {
      Socket socket = listener.accept();
      PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
      long curr = 1680438300000L;
      while (true) {
        if(sc.hasNextLine()) {
          String[] eachLine = sc.nextLine().split(",");
          printWriter.println(
                  eachLine[0]
                          + ","
                          + eachLine[1]
                          + ","
                          + eachLine[2]
                          + ","
                          + eachLine[3]
                          + ","
                          + eachLine[4]
                          + ","
                          + eachLine[5]
                          + ","
                          + eachLine[6]
                          + ","
                          + eachLine[7]
                          + ","
                          + curr);
          curr = curr + 30000;
          Thread.sleep(10000);
        }
      }
    } catch (IOException | NumberFormatException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
