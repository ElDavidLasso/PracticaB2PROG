import java.io.File
import kantan.csv._
import kantan.csv.ops._
import scala.io.Codec
import scala.collection.immutable.ListMap
import kantan.csv.generic._

implicit val code:Codec = Codec.ISO8859
case class Matricula(
                      provincia:String,
                      clase:String,
                      combustible: String,
                      marca: String,
                      servicio: String,
                      modelo: String,
                      tonelaje: Double,
                      asientos:Int,
                      estratone: String,
                      estrasientos: String
                    )
val path2DataFile = "C:/Users/LENOVO/Desktop/PROYECTO/datos2008.csv"

val dataSource = new File(path2DataFile).readCsv[List, Matricula](rfc.withHeader())
val rows = dataSource.filter(row => row.isRight)
val values = rows.collect({ case Right(matricula) => matricula})

//Cual es la Distribucion de los vehiculos clasificados segun el tipo de combustible que usan
val combustibleClases = values.map(row => (row.combustible, row.clase))
  .groupBy(identity).map({case (x , y)=> (x,y.length)})
combustibleClases.foreach(data => printf("%s,%d\n",data._1,data._2))
new File("C:\\Users\\LENOVO\\Desktop\\Programacion\\PracticaB2PROG\\data\\tipoCombus.csv")
.writeCsv[(String,String,Int)](
  combustibleClases.map(row => (row._1._1,row._1._2,row._2)),
  rfc.withHeader("Combustible", "Clase", "Cantidad")
)
//Cual es la distribucion de los vehiculos segun el servicio que prestan
val servicioVehi = values.map(row => (row.servicio, row.clase))
  .groupBy(identity).map({case (x , y)=> (x,y.length)})
servicioVehi.foreach(data => printf("%s,%d\n",data._1,data._2))
new File("C:\\Users\\LENOVO\\Desktop\\Programacion\\PracticaB2PROG\\data\\sevicioVehi.csv")
.writeCsv[(String, String, Int)](
  servicioVehi.map(row => (row._1._1, row._1._2, row._2)),
  rfc.withHeader("Servicio", "Clase", "Cantidad")
)
//Cual es la distribucion de los vehiculos segun la clase de vehiculo
val distriVehi = values.map(row => row.clase)
  .groupBy(identity).map({case (x , y)=> (x,y.length)})
distriVehi.foreach(data => printf("%s,%d\n",data._1,data._2))
new File("C:\\Users\\LENOVO\\Desktop\\Programacion\\PracticaB2PROG\\data\\distriVehi.csv")
.writeCsv[(String, Int)](
  distriVehi.map(row => (row._1, row._2)),
  rfc.withHeader("Servicio", "Cantidad")
)
//Cual es el numero de motoclicletas matriculadas en 2008 por provincia
val motos2008 = values.filter(row => row.clase == "Motocicleta" && row.modelo == "2008")
  .map(row => row.provincia).groupBy(identity).map({case (x , y)=> (x,y.size)})
motos2008.foreach(data => printf("%s,%d\n",data._1,data._2))
new File("C:\\Users\\LENOVO\\Desktop\\Programacion\\PracticaB2PROG\\data\\motos2008.csv")
.writeCsv[(String, Int)](
  motos2008.map(row => (row._1, row._2)),
  rfc.withHeader("Provincia", "Cantidad")
)
//Cual es la distribucion general y por provincia de los tipos de servicio para vehiculos que puedan considerarse de trabajo pesado(camion,tanquero,trailer,volqueta)
val distribgeneral = values.filter(row => row.clase == "Tanquero" | row.clase == "Trailer" | row.clase == "Volqueta" | row.clase.startsWith("Cami")).groupBy(_.clase)
val prov = values.filter(row => row.clase == "Tanquero" | row.clase == "Trailer" | row.clase == "Volqueta" | row.clase.startsWith("Cami"))
  .map(row => (row.clase,row.provincia)).groupBy(identity).map({case (x , y)=> (x,y.size)})
prov.foreach(data => printf("%s,%d\n",data._1,data._2))
new File("C:\\Users\\LENOVO\\Desktop\\Programacion\\PracticaB2PROG\\data\\provTrPesado.csv")
.writeCsv[(String, String, Int)](
  prov.map(row => (row._1._1, row._1._2, row._2)),
  rfc.withHeader("Clase", "Provincia", "cantidad")
)