# Informe - Laboratorio 3

## Ejercicio 1 — Identificar las regiones paralelizables

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


