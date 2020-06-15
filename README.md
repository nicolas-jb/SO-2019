# SO-2019
Operating Systems Subject- Integrative Practical Work - UTN


El objetivo del trabajo práctico consiste en desarrollar una solución que permita la simulación de
ciertos aspectos de una base de datos distribuida, es decir donde los distintos recursos del sistema
puedan realizar una carga de trabajo ejecutando desde distintos puntos.

Dichos componentes incluidos dentro de la arquitectura del sistema deberán trabajar en conjunto
para la planificación y ejecución de distintos scripts. Estos scripts estarán conformados por
múltiples Requests (operaciones como leer valores, escribirlos, crear tablas, etc), los cuales se
asemejan a las principales operaciones de una base de datos.
Los componentes del sistema serán:

* un kernel.
* un pool de memorias.
* un file system.

Los Request principales que le podremos solicitar al sistema son:

* **SELECT:** Permite la obtención del valor de una key dentro de una tabla.
* **INSERT:** Permite la creación y/o actualización de una key dentro de una tabla.
* **CREATE:** Permite la creación de una nueva tabla dentro del file system.
* **DESCRIBE:** Permite obtener la Metadata de una tabla en particular o de todas las tablas que el file
system tenga.
* **DROP:** Permite la eliminación de una tabla del file system.

## Proceso Kernel ⚙️
El proceso Kernel, será el punto de entrada a nuestro sistema. Él tendrá la responsabilidad de planificar las distintas request que se
quieran efectuar, así como también la administración del pool de memorias y distribución de los pedidos entre las mismas.
Las request pueden llegar a este componente de dos formas posibles:
* 1. Archivos LQL (Lissandra Query Language).
* 2. Request unitarias realizadas mediante la API de este componente, las cuales serán consideradas como archivos LQL de una única línea.

Al tener la capacidad de trabajar con varios scripts al mismo tiempo en sus memorias, el sistema permitirá que haya multiprocesamiento, es decir, múltiples scripts al mismo tiempo en estado “Exec” . El grado de multiprocesamiento estará dado por una entrada en el archivo de configuración, que será independiente de las memorias que tenga conectadas.
Una vez que un script ingresa a un estado de Exec el Kernel se encargará entonces de leer y parsear la siguiente línea y mandarla a ejecutar en alguna de las memorias del pool.
Para saber a qué memoria deberá enviar cada request el Kernel utilizará un criterio distinto dependiendo del tipo de consistencia de la tabla que se requiera. Existen 3 criterios:
* Strong: 1 sóla memoria asociada.
* Eventual: N memorias asociadas.
* Strong-Hash: N memorias asociadas pero dependiente de lla key del dato. Cada key siempre consulta a la misma memoria.

## Proceso Pool de Memorias ⚙️
Para soportar una alta demanda de pedidos no es posible contar con una única memoria (sería un cuello de botella que lo único que lograría es que nuestro sistema no funcione lo suficientemente rápido).

Por esta razón, se tendrán múltiples instancias de los procesos Memorias funcionando en simultáneo donde cada una funcionará independiente de otra.
Toda memoria conocerá por lo menos a una más (que estará configurada por archivo de configuración) a las cuales llamaremos seeds (o semillas).

Ahora, si cada una funciona independientemente de otra y dos memorias pueden trabajar sobre la misma tabla, esto genera una posible condición de carrera sobre los datos modificados y obtenidos. Esto se traduce en poder llegar a obtener un valor inconsistente al consultarlo a una memoria en particular cuando se actualizó recientemente en otra memoria. Esto es algo que puede pasar y estamos dispuestos a tomar como riesgo, ya que al enviar los datos al LFS siempre terminará impactándose el último valor actualizado, obtenido a
partir de la comparación por Timestamp.

Para administrar este último punto es que cada tabla tendrá distintos tipos de consistencia para poder verificar y validar los distintos casos. Por último, el sistema en general debe soportar la desconexión de una memoria. En caso que suceda, el proceso deberá identificar ésto y al enterarse el kernel quitar dicha memoria de los criterios que la utilizaban.

## Proceso File System ⚙️

El proceso File System será el encargado de persistir y administrar las distintas tablas del sistema.

El sub-módulo Lissandra será el punto de entrada al LFS, administrará la conexión con los demás módulos y entenderá como resolver cada uno de los distintos pedidos que se le soliciten.

El sub-módulo File System, será el encargado de resolver la persistencia de los archivos de las tablas en disco. Para ello, se dispondrá la especificación de un File System desarrollado especialmente para este trabajo práctico.

Por último, el sub-módulo Compactador será el encargado de realizar la compactación de las distintas tablas cuando el sistema lo requiera.

El proceso LFS deberá poder atender múltiples peticiones en simultáneo. Dentro del sistema las tablas únicamente pueden quedar bloqueadas mientras se realiza el proceso de compactación.
  
