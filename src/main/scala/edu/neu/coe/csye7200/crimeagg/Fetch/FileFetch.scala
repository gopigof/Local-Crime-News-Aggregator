package io.forward.ftp

import java.io.{File, FileOutputStream, InputStream}
//import org.apache.commons.net.ftp._
import org.apache.hadoop.shaded.org.apache.commons.net.ftp.FTPClient

import scala.util.Try

final class FTP() {
  private val client = new FTPClient

  def login(username: String, password: String): Try[Boolean] = Try {
    client.login(username, password)
  }

  def connect(host: String): Try[Unit] = Try {
    client.connect(host)
  }

  def connected: Boolean = client.isConnected

  def disconnect(): Unit = client.disconnect()

//  def canConnect(host: String): Boolean = {
//    client.connect(host)
//    val connectionWasEstablished = connected
//  }

  ///def listFiles(dir: Option[String] = None): List[FTPFile] = dir.fold(client.listFiles)(client.listFiles).toList

//  def connectWithAuth(host: String, username: String = "anonymous", password: String = ""): Try[Boolean] = {
//    for {connection <- connect(host)
//  }

  def cd(path: String): Boolean = client.changeWorkingDirectory(path)

//  def filesInCurrentDirectory: Seq[String] = listFiles().map(_.getName)
//
//  def downloadFileStream(remote: String): InputStream = {
//    val stream = client.retrieveFileStream(remote) client.completePendingCommand() stream
//  }

//  def downloadFile(remote: String): Boolean = {
//    val os = new FileOutputStream(new File(remote)) client
//    .retrieveFile(remote, os)
//  }

  def uploadFile(remote: String, input: InputStream): Boolean = client.storeFile(remote, input)
}