#ifndef SRC_LFS_H_
#define SRC_LFS_H_

#include <commons/collections/list.h>
#include <commons/bitarray.h>
#include <commons/txt.h>
#include <commons/config.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <sys/inotify.h>

#define PATH_CONFIG "/home/utnso/Escritorio/tp-2019-1c-dale_que_SO_vo/LFS/configuracion_LFS.config"
// El tamaño de un evento es igual al tamaño de la estructura de inotify
// mas el tamaño maximo de nombre de archivo que nosotros soportemos
// en este caso el tamaño de nombre maximo que vamos a manejar es de 24
// caracteres. Esto es porque la estructura inotify_event tiene un array
// sin dimension ( Ver C-Talks I - ANSI C ).
#define EVENT_SIZE  ( sizeof (struct inotify_event) + 24 )

// El tamaño del buffer es igual a la cantidad maxima de eventos simultaneos
// que quiero manejar por el tamaño de cada uno de los eventos. En este caso
// Puedo manejar hasta 1024 eventos simultaneos.
#define BUF_LEN     ( 1024 * EVENT_SIZE )

typedef struct {
	char* nombre_tabla;
	pthread_mutex_t semaforo_c;
	pthread_mutex_t semaforo_particion;
} Semaforo_de_una_tabla;

t_list* lista_semaforos;

typedef struct {
	long timestamp;
	uint16_t key;
	char* value;
	int tamanio_registro;
	int particion_destino;
} Registro_compactacion;

typedef struct {
	long timestamp;
	uint16_t key;
	char* value;
} Registro;

typedef struct {
	char* nombre;
	t_list* registros;
} Tabla;

typedef enum {
	SC = 1, SHC = 2, EC = 3
} TIPO_CONSISTENCIA;

typedef struct {
	char* ip;
	char* puerto;
	char* puerto_escucha;
	char* punto_montaje;
	int retardo;
	int tamanio_value;
	int tiempo_dump;
} Configuracion_LFS;

t_config* config;

//--------------------
int lfs_encendido; //FIXME FLAG APAGADO

//-----------------
void* leer_consola();
int analisis_instruccion(char* instruccion_leida);

void* notificaciones();
void cargar_parametros_modificados();

pthread_mutex_t mutex_bloque;
pthread_mutex_t mutex_socket;
pthread_mutex_t mutex_bitarray;
pthread_mutex_t mutex_ingreso_archivo_config;
pthread_mutex_t mutex_memtable;
pthread_mutex_t mutex_lista_semaforos;


pthread_t hilo_notificador;

pthread_t hilo_compactacion;
t_list* lista_tablas_lfs;

//-----------------

pthread_t hilo_consola;
pthread_t hilo_lfs;
pthread_t hilo_dump;
pthread_attr_t attr;

t_bitarray* bitarray;
char* cadena_bitarray;

Configuracion_LFS* configuracion_lfs;
t_list* memtable;

char* validar_consistencia(TIPO_CONSISTENCIA consistencia);

char* nombre_punto_montaje;

void iniciar_punto_montaje();

// Metadata:
void leer_metadata_LFS();
int cantidad_bloques_LFS;
int tamanio_bloque_LFS;
char* magic_number;


int bloque_glob;
//SELECT
t_list* leer_datos_de_particion(char* path_tabla, int numero_particion);

//INSERT
int existe_tabla_en_memtable(char* nombre_tabla);

int existe_archivo(char* path_archivo);

int existe_carpeta(char* nombre_carpeta);

void crear_fichero(char* path);

void* recibir_clientes();

void iniciar_memtable();
void limpiar_memtable();
void eliminar_tabla(Tabla* tabla);
void eliminar_registro(Registro* registro);
Tabla* crear_tabla(char* nombre_tabla);
Tabla* buscar_tabla(char* nombre_tabla);
Registro* buscar_registro(Tabla* tabla, uint16_t key);
Registro* crear_registro(uint16_t key, char* value, long timestamp);
void agregar_registro_a_tabla(Tabla* tabla, Registro* registro);
int guardar_en_memtable(char*nombre_tabla, uint16_t key, char* value,
		long timestamp);

char* directorio_tablas;
char* directorio_bloques;
char* directorio_bitmap;
char* directorio_de_tablas();
char* directorio_de_bloques();
void inicializar_directorios();

char* seleccionar_bloque_a_manipular(int numero_de_bloque);
char* seleccionar_particion_a_manipular(int numero_de_particion,
		char* directorio_de_tabla_determinada);
int devolver_numero_de_primer_bloque_no_ocupado();
int cantidad_de_bloques_vacios();
void iniciar_bitmap(int cant_bloques);
void mostrar_bitarray();
int cant_bloques;
//DROP

void eliminar_particion_y_vaciar_bloques(char* path_tabla, int numero_particion);
void eliminar_temporal_y_vaciar_bloques(char* path_tabla, int numero_temporal);
void vaciar_bloque(char* path);
t_list* crear_listado_archivos_en_tablas(char* path_tabla);

void inicializar_nombre_punto_montaje();  			 //OJO VA POR ARCHIVO CONFIG

void leer_config();

t_list* crear_listado_nombres_tablas();

char* quitar_barra_n(char* cadena);

//DUMP
void* dump_lfs();
void dump();
void abrir_archivo_y_escribir_linea(char* path_archivo, char* linea);
char* armar_linea_registro(Registro* registro);
int hay_espacio_para_dumpear_toda_la_tabla(Tabla* tabla_en_memtable);
int tamanio_que_ocupara_dump_tabla(Tabla* tabla_en_memtable);
t_list* crear_lista_sin_registros_repetidos(t_list* lista_registros);

//DE COMPACTACION
void compactar(char* nombre_tabla_a_compactar);
void* compactar_lfs(void* nombre_tabla);
void renombrar_archivos_temporales(char* path_viejo);
char* armar_path_de_archivo_temporal(int numero_archivo_temporal,
		char* path_tabla);
char* armar_path_de_archivo_temporal_listo_compactar(
		int numero_archivo_temporal, char* path_tabla);
char* leer_datos_de_bloque(char* path_bloque);
Registro_compactacion* deserializar_registro(char* linea);
Registro_compactacion* deserializar_registro_temporal(char* linea,
		int cantidad_particiones);
char* armar_path_particion(char* path_tabla, int numero_particion);
int buscar_key_en_particion(char* path_tabla, int numero_particion,
		uint16_t key);
char* aniadir_bloque_asignado(char* path_particion_o_temporal);
t_list* agregar_registro_repetido_a_lista(t_list* lista_registros_de_particion,
		Registro_compactacion* registro_compactacion);
t_list* agregar_registro_no_repetido_a_lista(
		t_list* lista_registros_de_particion,
		Registro_compactacion* registro_compactacion);
void eliminar_registro_compactacion(Registro_compactacion* registro);
char* armar_linea_registro_compactacion(Registro_compactacion* registro);
void escribir_lista_registros_en_bloque(t_list* lista, char* path_bloque,
		char* path_particion);
int espacio_disponible_en_bloque(char* path_bloque);
char* bloque_con_espacio(char* path_particion, char* linea);
int cantidad_lineas(char* path);
void guardar_en_lista_registros_de_archivo(
		char* path_archivo_particion_o_temporal, t_list* lista_registros,
		int cantidad_particiones);
void iniciar_compactaciones_de_tablas_existentes();
void escribir_lista_registros_en_particion(t_list* lista_registros, char* path_particion);

//INSERT
int determinar_particion(int numero_key, int cantidad_particiones);

typedef struct {
	char* nombre_tabla;
	char* tipo_consistencia;
	char* particiones;
	char* tiempo_compactacion;
} Datos_describe_tabla;

void eliminar_tabla(Tabla* tabla);

void finalizar();

TIPO_CONSISTENCIA convertir_consistencia(char* aux_consistencia);

#endif /* SRC_LFS_H_ */

