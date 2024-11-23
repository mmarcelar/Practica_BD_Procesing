import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

object Examen extends App {

  /**Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   estudiantes (nombre, edad, calificación).
   Realiza las siguientes operaciones:

   Muestra el esquema del DataFrame.
   Filtra los estudiantes con una calificación mayor a 8.
   Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  def ejercicio1(estudiantes: DataFrame)(spark:SparkSession): DataFrame = {

    // Muestra el esquema del DataFrame
    println(estudiantes.schema)

    // Filtra los estudiantes con una calificación mayor a 8.
    val estudiantes_filtrados = estudiantes.filter("calificacion > 8")

    // Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
    return estudiantes_filtrados.sort(desc("calificacion")).select("nombre")
  }

  /**Ejercicio 2: UDF (User Defined Function)
   Pregunta: Define una función que determine si un número es par o impar.
   Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */
  def ejercicio2(numeros: DataFrame)(spark:SparkSession): DataFrame =  {
    def es_par: (Int => Boolean) = { s => s % 2 == 0 }
    val es_par_udf = udf(es_par)
    return numeros.withColumn("es_calificacion_par", es_par_udf(numeros("calificacion")))
  }

  /**Ejercicio 3: Joins y agregaciones
   Pregunta: Dado dos DataFrames,
   uno con información de estudiantes (id, nombre)
   y otro con calificaciones (id_estudiante, asignatura, calificacion),
   realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */
  def ejercicio3(estudiantes: DataFrame , calificaciones: DataFrame)(spark:SparkSession): DataFrame = {
    val joined_df = estudiantes.join(calificaciones).where("id == id_estudiante")
    return joined_df.groupBy("id_estudiante").agg( "calificacion" -> "avg")
  }

  /**Ejercicio 4: Uso de RDDs
   Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.
   */
  def ejercicio4(palabras: List[String])(spark:SparkSession): RDD[(String, Int)] = {
    val rdd = spark.sparkContext.parallelize(palabras)
    return rdd.map(x => (x,1)).reduceByKey(_+_)
  }

  /**
   Ejercicio 5: Procesamiento de archivos
   Pregunta: Carga un archivo CSV que contenga información sobre
   ventas (id_venta, id_producto, cantidad, precio_unitario)
   y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */
  def ejercicio5(ventas: DataFrame)(spark:SparkSession): DataFrame = {
    def producto: ((Int,Int) => Int) = { (x, y) => x * y }
    val producto_udf = udf(producto)
    val df = ventas.withColumn("precio_venta_total", producto_udf(ventas("cantidad"), ventas("precio_unitario")))
    return df.groupBy("id_producto").agg( "precio_venta_total" -> "sum")
  }
}
