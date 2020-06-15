#ifndef SRC_KERNEL_H_
#define SRC_KERNEL_H_

#include "API-Kernel.h"
#include "notify.h"
#include <pthread.h> // -lpthread
#include "sockets.h"
#include <semaphore.h>
#include <sys/inotify.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/inotify.h>
#include <commons/string.h>
#include <string.h>
#include <commons/collections/list.h>
#include "metricas.h"
#include <time.h>

// Variables globales
t_log* logger;
t_config* config;

sem_t s_espera_conexion_con_pool;
sem_t s_hay_elementos_en_new;
sem_t s_hay_elementos_en_ready;
sem_t s_se_termino_un_exec;
sem_t s_espero_el_primer_describe;

pthread_t hilo_consola;
pthread_t hilo_notif;
pthread_t hilo_cola_ready;
pthread_t hilo_cola_ejex;
pthread_t hilo_metricas;
pthread_t hilo_consulta_memorias;
pthread_t hilo_describe_scheduleado;
pthread_mutex_t mutex_id_script;
pthread_mutex_t mutex_ingreso_archivo_config;
pthread_mutex_t mutex_lectura_memorias;
pthread_mutex_t mutex_exec;
pthread_mutex_t mutex_metricas;
pthread_mutex_t mutex_metadata;
pthread_mutex_t mutex_memorias;
pthread_mutex_t mutex_carrera;

int flag_primera_vez;
int flag_primer_describe;

struct Config* configuracion_kernel;

struct Config {
	char* ip_m;
	char* puerto_m;
	int quantum;
	int multiprocesamiento;
	int metadata_refresh;
	int sleep_ejecucion;
	int sleep_memorias;
	int run_metrics;
};

typedef struct {
	char* nombre_tabla;
	char* tipo_consistencia;
	char* particiones;
	char* tiempo_compactacion;
}S_Metadata;


t_list* metadata;

typedef struct {
	char* puerto_m; //id es el puerto
//	int estado; //Si ya la estoy usando para una request o no
	char* ip_m;
	int id;
} S_Memoria;

typedef struct {
	//int cantidad_operaciones;
	float cantidad_inserts;
	float cantidad_selects;
	time_t t0_insert;
	time_t tf_insert;
	double duracion_insert;
	time_t t0_select;
	time_t tf_select;
	double duracion_select;
	float memory_load;
} S_Consistency;

typedef enum {
	OCUPADA, DISPONIBLE
} Estado_M;

//Consistencias globales para mostrar en las métricas
S_Consistency* SC;
S_Consistency* EC;
S_Consistency* SHC;

//Variable global para conocer a todas las memorias
int id_memoria;

//Memorias que me va a ir informando el pool que va levantando
t_list* memorias;
t_list* memorias_aux;
t_list* criterio_SC;
t_list* criterio_SHC;
t_list* criterio_EC;

//Diccionario para las SHC Memorys
t_dictionary* diccionario_shc_memorias;

void* obtener_la_memoria(t_list*, S_Memoria*);
int conozco_la_memoria(t_list*, S_Memoria*);
void remover_de_lista(t_list*, S_Memoria*);
void limpiar_consistencias(S_Consistency*);
void eliminar_de_lista_de_memorias(S_Memoria*);

void destructor_elementos_lista(S_Memoria*);
void destructor_elementos_lista_meta(S_Metadata*);
S_Memoria* buscar_memoria_por_id(S_Memoria*);

#endif /* SRC_KERNEL_H_ */
