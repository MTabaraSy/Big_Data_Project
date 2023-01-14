package Kafkatransit


import org.apache.spark.sql.DataFrame

import scala.Producer.{DFsociete, DFtours}

object Queries extends App {

  //question 1
  val societeCount  = DFtours.select("company_permalink").distinct().count()
  println(societeCount)

  //question 2
  val societeCount2 = DFsociete.select("permalink").distinct().count()
  println(societeCount2)

  //question 3
  //permalink peut être utilisé pour comme clé dans le dataframe societesDF

  //question 4
  val toursociete = DFtours.select("company_permalink").distinct()
  val societeSoc = DFsociete.select("permalink").distinct()
  val notInSociet :DataFrame = toursociete.subtract(societeSoc)
  notInSociet.show()
  //oui il existe des sociètes dans toursDF qui ne sont pas présentes dans societeDF

  //Question 5
  val unionDFs = DFtours.union(DFsociete)










}
