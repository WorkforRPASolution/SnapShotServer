package com.sec.eeg.ars.data

case class UPLOADFILES(hostname: String, filename: String, image: String)
case class DIRSNAPSHOT(hostname: String, year: Int, month: Int)
case class GETFILE(hostname: String, year: Int, month: Int, filename: String)
case class HTTPRETURN(result: String, message: String)
case class DIRRETURN(result: String, message: String, filelist: List[String])
case class IMGRETURN(result: String, message: String, img: String)