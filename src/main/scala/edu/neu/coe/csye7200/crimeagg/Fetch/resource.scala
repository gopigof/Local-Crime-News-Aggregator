import org.apache.hadoop.util.JsonSerialization.writer

import java.io.File
import java.io.FileOutputStream
import java.io.FileReader
import java.io.InputStream
import java.io.IOException
import java.net.URL
import java.util.regex.Pattern
import javax.swing.text.html.HTML
import javax.swing.text.html.HTMLEditorKit
import javax.swing.text.html.parser.ParserDelegator
import javax.swing.text.MutableAttributeSet;

object Extracter {
  def download(filename: String) = {
    var url = new URL(filename);
    var base = basename(filename);
    if (!exists(base)) {
      var connection = url.openConnection();
      var total = connection.getContentLength();
      var stream = connection.getInputStream();
      Console.println("Downloading " + filename);
      var bytes = new Array[Byte](500000);
      var x: Long = 0;
      var writer = new FileOutputStream(base);
      try {
        while (true) {
          if (total > 100) {
            var pc = (x / (total / 100));
            Console.print("Got " + x + " of " + total + " (" + pc + "%)\r");
          }
          var read = stream.read(bytes);
          if (read > 0) {
            writer.write(bytes, 0, read);
            x += read;
          } else {
            throw new IOException("meh");
          }
        }
      } catch {
        case e: IOException => Console.println("\nDone");
      }
    }
  }

  def basename(url: String): String = {
    if (url.endsWith("/")) {
      return "index.html"
    }
    var name = (new File(url)).getName;
    return name match {
      case "" => "index.html"
      case _ => name;
    }
  }

  def exists(filename: String): Boolean = {
    return (new File(filename)).exists;
  }

  def fakedownload(filename: String) = {
    Console.println("Downloaded " + filename);
  }

  class PrintLinks(pattern: Pattern) extends HTMLEditorKit.ParserCallback {
    override def handleStartTag(t: HTML.Tag, a: MutableAttributeSet, pos: Int) = {
      if (t == HTML.Tag.A) {
        var src = a.getAttribute(HTML.Attribute.HREF);
        var matcher = pattern.matcher(src.toString);
        if (matcher.matches) {
          Console.println(src);
          download(src.toString());
          // fakedownload(src.toString);
        }
      }
    }
  }

  def main(args: Array[String]) = {
    try {
      var inpattern = args.length match {
        case 2 => args(1) + "."
        case _ => ""
      }
      var inputfile = args(0); if (inputfile.startsWith("http:")) {
        Console.println(basename(inputfile)); download(inputfile); inputfile = basename(inputfile);
      }
      var pattern = Pattern.compile("." + inpattern);
      var reader = new FileReader(inputfile);
      var callback = new PrintLinks(pattern); new ParserDelegator().parse(reader, callback, true);
    } catch {
      case e: ArrayIndexOutOfBoundsException => Console.println("Usage: extract FILENAME [pattern]");
      case e: IOException => Console.println(e.getMessage());
    }
  }
}

