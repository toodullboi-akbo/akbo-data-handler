import java.io.File

object FileMatcher {
  private def filesHere: Array[File] = ( new File((".")).listFiles )

  def filesMatching(query : String, matcher : (String, String) => Boolean): Array[File] = {
    for ( file <- filesHere; if matcher(file.getName, query))
      yield file
  }
}
