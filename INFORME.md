# Informe - Laboratorio 3

## Ejercicio 1 — Identificar las regiones paralelizables

### 1.a - Dibujar diagrama de flujo con los pasos que debe realizar el programa

```
[Driver] Leer JSON y paralelizar (sc.parallelize)
   |
   | ---> Tipo de conexión: RDD[Subscription]
   v
[Workers] Descargar feeds y extraer posts (flatMap)
   |
   | ---> Tipo de conexión: RDD[Post]
   v
[Workers] Extraer entidades nombradas (flatMap)
   |
   | ---> Tipo de conexión: RDD[NamedEntity]
   v
[Workers] Clasificar y preparar pares clave-valor (map)
   |
   | ---> Tipo de conexión: RDD[((String, String), Int)]
   v
[Workers] Contar apariciones sumando valores (reduceByKey)
   |
   | ---> Tipo de conexión: RDD[((String, String), Int)]
   v
[Workers] Ordenar resultados por conteo y tipo (sortBy)
   |
   | ---> Tipo de conexión: RDD[((String, String), Int)]
   v
[Driver] Recolectar resultados (collect) e imprimir
   |
   | ---> Tipo final en memoria: Array[((String, String), Int)]
```

### 1.b

Descarga de feeds y extracción de posts: Corresponde a flatMap. La justificación es que una sola URL de suscripción puede generar múltiples posts, o incluso cero si la descarga falla o si los posts se filtran por estar vacíos.
  
Extracción de entidades nombradas: Corresponde a flatMap. De un solo post pueden salir varias namedEntity, o ninguna si el diccionario no detecta coincidencias relevantes.  

Clasificación y preparación de pares clave-valor: Corresponde a map. Cada entidad extraída se transforma independientemente en exactamente un par de la forma ((tipo, nombre), 1).  

Conteo de apariciones: Corresponde a reduceByKey. Combina múltiples elementos distribuidos agrupando todas las tuplas que tienen la misma clave (la misma entidad) para sumar sus valores y dejar un solo resultado por entidad.  

¿Hay pasos que no encajen en estas abstracciones?

Sí, en el pipeline de dependencias hay pasos que no encajan ni en map, ni flatMap, ni reduceByKey:  

La recolección de resultados (collect): Las abstracciones como map o flatMap son transformaciones que crean un nuevo RDD de forma perezosa. En cambio, collect y las impresiones por pantalla son acciones terminales. Estas no transforman los datos de forma distribuida, sino que fuerzan la ejecución del pipeline y traen los datos desde los workers de vuelta a la memoria del driver.

La inicialización (sc.parallelize): No transforma un RDD existente, sino que es la operación de origen que toma una colección local del driver y la inyecta al sistema distribuido.

El ordenamiento global (sortBy): Aunque es una transformación, tiene una lógica distinta. No procesa elementos de forma totalmente independiente (map) ni agrupa estrictamente para reducir un valor (reduceByKey), sino que requiere intercambiar datos por toda la red para establecer un orden total de mayor a menor aparición.

### 1.c

Pasos completamente independientes: La descarga de feeds (flatMap), la extracción de entidades (flatMap) y la clasificación (map) se ejecutan de manera "vergonzosamente paralela". Cada worker agarra un post, lo procesa y escupe un resultado sin importarle qué están haciendo los demás workers ni necesitar comunicarse con ellos.

Barreras de sincronización: El punto de quiebre es el reduceByKey. Constituye una barrera porque Spark necesita agrupar los datos por clave a través de toda la red, un proceso conocido como shuffle. Ningún worker puede emitir el resultado final del conteo para una entidad específica hasta que todos los workers del clúster hayan terminado sus etapas de map y hayan enviado sus conteos parciales. El ordenamiento final (sortBy) también actúa como barrera por el mismo motivo: necesitás ver todos los datos para ordenarlos globalmente.  

### 1.d

Serialización: Las funciones (y cualquier objeto o variable que referencien de su entorno exterior) tienen que ser serializables. Si se referencia un objeto que no puede convertirse a bytes para viajar por la red, Spark va a tirar una excepción antes de arrancar.

Estado Compartido: En un clúster no hay memoria física compartida como ocurre con los hilos locales de un sistema operativo. Si la función intenta modificar una variable global normal, cada worker va a modificar su propia copia local aislada y los resultados se van a perder. Para compartir estado o recolectar métricas se deben usar mecanismos específicos como los Accumulators.

Efectos Secundarios: Las funciones deben ser puras en lo posible. Spark provee tolerancia a fallos reejecutando tareas que se caen. Si tu función tiene efectos secundarios (por ejemplo, hace un insert directo a una base de datos externa), una caída del worker y su posterior reejecución podría causar que se inserten datos duplicados o que el estado quede corrupto.

## Ejercicio 2 — Paralelizar la descarga de feeds

## Ejercicio 3 — Paralelizar el cómputo de entidades nombradas

### ¿Qué ocurre en el cluster en `reduceByKey`? ¿Por qué es inevitable para este problema?

Cuando Spark llega al `reduceByKey`, ejecuta una operación de **shuffle**: redistribuye todos los pares clave-valor entre los workers de forma que todos los pares con la misma clave terminen en el mismo worker. Recién entonces ese worker puede sumar los conteos parciales y producir el resultado final por entidad.

Esta barrera de sincronización es inevitable porque el conteo de una entidad como `("ProgrammingLanguage", "Python")` puede estar distribuido entre múltiples workers, ya que cada uno procesó un subconjunto distinto de posts. Ningún worker tiene el total por sí solo, por lo que todos deben sincronizarse e intercambiar sus resultados parciales antes de poder producir el conteo definitivo.

### ¿Qué restricciones debe cumplir la función que se le pasa a `reduceByKey`?

La función debe ser **asociativa** y **conmutativa**.
Asociativa porque Spark puede combinar valores en distintos órdenes dependiendo de cómo distribuya el trabajo entre workers.
Conmutativa porque Spark no garantiza en qué orden llegan los valores a cada worker durante el shuffle.
Si la función no cumpliera estas propiedades, el resultado podría variar entre ejecuciones dependiendo del orden en que Spark procese los datos. La suma `(x, y) => x + y` cumple ambas condiciones, lo que la hace segura para usar en un entorno distribuido.

### ¿Dónde se hace la lectura del diccionario de entidades? ¿En el driver o los workers?

El diccionario se lee en el **driver**, mediante `Dictionary.loadAll(cmdArgs.entitiesDir)`, antes de que comience el `flatMap`. Spark serializa el diccionario y lo envía a cada worker como parte del contexto de la tarea.

Si la lectura se realizara dentro del `flatMap`, cada worker intentaría acceder al archivo desde su propio sistema de archivos. En un cluster real, los workers son máquinas distintas que no tienen acceso al filesystem del driver, por lo que la lectura fallaría. Leer el diccionario en el driver y dejar que Spark lo distribuya es el patrón correcto para este tipo de dato de referencia compartido.


