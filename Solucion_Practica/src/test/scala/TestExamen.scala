import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

class TestExamen extends AnyFunSuite {

  val spark: SparkSession = SparkSession.builder().appName("SparkTest").master("local[1]").getOrCreate()

  test("Practica.ejercicio1") {
    val estudiantes_input = List(
      ("Maria Perez", 11, 7),
      ("Pedro Rodriguez", 10, 6),
      ("Ramon Ramirez", 12, 8),
      ("Antonio Martinez", 9, 9),
      ("Lucrecia Pereira", 12, 10),
      ("Antonella Paez", 9, 8),
      ("Ruben Sanchez", 10, 4),
      ("Roberto Tapia", 11, 10),
      ("Jose Gutierrez", 10, 6),
      ("Raul Gomez", 10, 9)
    )
    val rdd = spark.sparkContext.parallelize(estudiantes_input)
    import spark.implicits._
    //  RDD -> DF
    val estudiantes_input_df = rdd.toDF("nombre", "edad", "calificacion")

    val resultado = Examen.ejercicio1(estudiantes_input_df)(spark).collectAsList()

    assert(resultado.size() == 4)
    assert(resultado.get(0).getString(0) == "Lucrecia Pereira")
    assert(resultado.get(1).getString(0) == "Roberto Tapia")
    assert(resultado.get(2).getString(0) == "Antonio Martinez")
    assert(resultado.get(3).getString(0) == "Raul Gomez")
  }

  test("Practica.ejercicio2") {

    val estudiantes_input = List(
      ("Maria Perez", 11, 7),
      ("Pedro Rodriguez", 10, 6),
      ("Ramon Ramirez", 12, 8),
      ("Antonio Martinez", 9, 9),
      ("Lucrecia Pereira", 12, 10),
      ("Antonella Paez", 9, 8),
      ("Ruben Sanchez", 10, 4),
      ("Roberto Tapia", 11, 10),
      ("Jose Gutierrez", 10, 6),
      ("Raul Gomez", 10, 9)
    )
    val rdd = spark.sparkContext.parallelize(estudiantes_input)
    import spark.implicits._
    //  RDD -> DF
    val estudiantes_input_df = rdd.toDF("nombre", "edad", "calificacion")

    val resultado = Examen.ejercicio2(estudiantes_input_df)(spark).collectAsList()

    assert(resultado.size() == 10)
    assert(resultado.get(0).getAs("es_calificacion_par").equals(false))
    assert(resultado.get(1).getAs("es_calificacion_par").equals(true))
    assert(resultado.get(2).getAs("es_calificacion_par").equals(true))
    assert(resultado.get(3).getAs("es_calificacion_par").equals(false))
    assert(resultado.get(4).getAs("es_calificacion_par").equals(true))
    assert(resultado.get(5).getAs("es_calificacion_par").equals(true))
    assert(resultado.get(6).getAs("es_calificacion_par").equals(true))
    assert(resultado.get(7).getAs("es_calificacion_par").equals(true))
    assert(resultado.get(8).getAs("es_calificacion_par").equals(true))
    assert(resultado.get(9).getAs("es_calificacion_par").equals(false))
  }

  test("Practica.ejercicio3") {

    val estudiantes_input = List(
      (1, "Maria Perez"),
      (2, "Pedro Rodriguez"),
      (3, "Ramon Ramirez"),
      (4, "Antonio Martinez"),
      (5, "Lucrecia Pereira")
    )
    val rdd = spark.sparkContext.parallelize(estudiantes_input)
    import spark.implicits._
    //  RDD -> DF
    val estudiantes_input_df = rdd.toDF("id", "nombre")

    val calificaciones_input = List(
      (1, "Matematica", 10), (1, "Biologia", 9), (1, "Geografia", 6),  (1, "Historia", 9),
      (2, "Matematica", 8), (2, "Biologia", 4), (2, "Geografia", 7),  (2, "Historia", 8),
      (3, "Matematica", 7), (3, "Biologia", 6), (3, "Geografia", 8),  (3, "Historia", 6),
      (4, "Matematica", 5), (4, "Biologia", 7), (4, "Geografia", 9),  (4, "Historia", 7),
      (5, "Matematica", 9), (5, "Biologia", 8), (5, "Geografia", 10),  (5, "Historia", 6)
    )
    val rdd_calificaciones = spark.sparkContext.parallelize(calificaciones_input)
    import spark.implicits._
    //  RDD -> DF
    val calificaciones_input_df = rdd_calificaciones.toDF("id_estudiante", "asignatura", "calificacion")

    val resultado = Examen.ejercicio3(estudiantes_input_df, calificaciones_input_df)(spark).collectAsList()

    assert(resultado.size() == 5)

    assert(resultado.get(0).getAs("id_estudiante").equals(1))
    assert(resultado.get(0).getAs("avg(calificacion)").equals(8.5))

    assert(resultado.get(1).getAs("id_estudiante").equals(3))
    assert(resultado.get(1).getAs("avg(calificacion)").equals(6.75))

    assert(resultado.get(2).getAs("id_estudiante").equals(5))
    assert(resultado.get(2).getAs("avg(calificacion)").equals(8.25))

    assert(resultado.get(3).getAs("id_estudiante").equals(4))
    assert(resultado.get(3).getAs("avg(calificacion)").equals(7.0))

    assert(resultado.get(4).getAs("id_estudiante").equals(2))
    assert(resultado.get(4).getAs("avg(calificacion)").equals(6.75))
  }

  test("Practica.ejercicio4") {

    val palabras = List(
      "casa", "auto", "casa", "flor", "vaso", "flor", "plato", "flor", "flor"
    )

    val resultado = Examen.ejercicio4(palabras)(spark).collect()

    assert(resultado.length == 5)

    assert(resultado(0)._1.equals("plato"))
    assert(resultado(0)._2.equals(1))

    assert(resultado(1)._1.equals("vaso"))
    assert(resultado(1)._2.equals(1))

    assert(resultado(2)._1.equals("auto"))
    assert(resultado(2)._2.equals(1))

    assert(resultado(3)._1.equals("flor"))
    assert(resultado(3)._2.equals(4))

    assert(resultado(4)._1.equals("casa"))
    assert(resultado(4)._2.equals(2))
  }

  test("Practica.ejercicio5") {

    val df = spark.read.format("csv").option("header", "true").load("src/test/resources/ventas.csv")
    val resultado = Examen.ejercicio5(df)(spark).collectAsList()

    assert(resultado.size() == 10)

    assert(resultado.get(0).getAs("id_producto").equals("101"))
    assert(resultado.get(0).getAs("sum(precio_venta_total)").equals(460L))

    assert(resultado.get(1).getAs("id_producto").equals("107"))
    assert(resultado.get(1).getAs("sum(precio_venta_total)").equals(396L))

    assert(resultado.get(2).getAs("id_producto").equals("110"))
    assert(resultado.get(2).getAs("sum(precio_venta_total)").equals(494L))

    assert(resultado.get(3).getAs("id_producto").equals("104"))
    assert(resultado.get(3).getAs("sum(precio_venta_total)").equals(800L))

    assert(resultado.get(4).getAs("id_producto").equals("102"))
    assert(resultado.get(4).getAs("sum(precio_venta_total)").equals(405L))

    assert(resultado.get(5).getAs("id_producto").equals("103"))
    assert(resultado.get(5).getAs("sum(precio_venta_total)").equals(280L))

    assert(resultado.get(6).getAs("id_producto").equals("108"))
    assert(resultado.get(6).getAs("sum(precio_venta_total)").equals(486L))

    assert(resultado.get(7).getAs("id_producto").equals("106"))
    assert(resultado.get(7).getAs("sum(precio_venta_total)").equals(425L))

    assert(resultado.get(8).getAs("id_producto").equals("105"))
    assert(resultado.get(8).getAs("sum(precio_venta_total)").equals(570L))

    assert(resultado.get(9).getAs("id_producto").equals("109"))
    assert(resultado.get(9).getAs("sum(precio_venta_total)").equals(540L))
  }
}
