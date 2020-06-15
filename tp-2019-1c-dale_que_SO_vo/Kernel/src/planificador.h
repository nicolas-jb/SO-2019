/*
 * planificador.h
 *
 *  Created on: 7 jun. 2019
 *      Author: utnso
 */

#ifndef SRC_PLANIFICADOR_H_
#define SRC_PLANIFICADOR_H_

#include <stdlib.h>
#include <stdio.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/collections/queue.h>
#include "kernel.h"
#include "API-Kernel.h"
#include "sockets.h"
#include <time.h>

//Diferentes colas del planificador
t_queue *cola_new;
t_queue *cola_ready;
t_queue *cola_execute;
t_queue *cola_exit;

int flag_eliminar_paquete;
unsigned int semilla;

// creo un id global para poder diferenciar todos los scripts, luego veo si es necesario o no
int id_scripts;
int cantidad_de_hilos_en_exec;

typedef struct {
	char* cod_instruccion; //Separo la instrucción porque no llega bien a la cola execute
	char* datos;
} Instruccion;

typedef struct {
	int id_script;
	char* cod_instruccion; //Separo la instrucción porque no llega bien a la cola execute
	char* datos;
	int tipo_script;     // para saber si es un archivo o consola
	char *path;
	int lineas_pendientes;
	int cantidad_de_lineas;
	int estado_script;	// es imp en los script de archivos para saber si una linea falla, sacarlo
} Script;

typedef enum {
	PENDIENTE, TERMINADO_CON_ERROR, TERMINADO_SIN_ERROR
} ESTADO;

typedef enum {
	ARCHIVO, CONSOLA
} TIPO_SCRIPT;

void init_estados_panif();
void* ejecutar();
void crear_hilos_de_ejecucion(int, int);
void* hilos_execute(void*);
void* pasar_a_ready();
Script* generar_script_una_linea(Instruccion*);
Script* generar_script_path(Instruccion*);
char* instruccion_del_archivo_segun_linea(int, char*);
int cantidad_lineas(char* path);
int minimo_entre(int, int);
int instruccion_a_pool(char* cod_instruccion);

void funcion_journal(Script*);
void funcion_add(Script*);
void funcion_run(Script*, int);
void funcion_metrics(Script*);
void funcion_del_pool(Script*);
void ejecutar_las_request(Script*, int);
Script* cargar_instruccion_del_archivo(Script* un_script);
void cargar_datos_metadata(char*);
void journal(t_list*);
void destructor_script(Script*);
void destructor_instr(Instruccion*);
void generar_diccionario();
char* obtener_consistencia_la_tabla(char*);
void* obtener_memoria(char*, char*, int); // El tercer parámetro indica si la instrucción es hasheable o no, si no lo es obtengo cualquier memoria
void* describe_scheduleado();

#endif /* SRC_PLANIFICADOR_H_ */

