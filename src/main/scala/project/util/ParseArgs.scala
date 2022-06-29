package project.util

import org.rogach.scallop.{ScallopConf, ScallopOption}

class ParseArgs(args: Seq[String]) extends ScallopConf(args){
 val mobilePath: ScallopOption[String] = opt[String](required = true)
 val userPath: ScallopOption[String] = opt[String](required = true)
 verify()
}