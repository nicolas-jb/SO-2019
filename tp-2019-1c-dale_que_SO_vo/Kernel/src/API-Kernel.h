#ifndef SRC_API_KERNEL_H_
#define SRC_API_KERNEL_H_

#define TAMANIO_MAX_INST 50
#define PATH_CONFIG "/home/utnso/Escritorio/tp-2019-1c-dale_que_SO_vo/Kernel/configuracion_kernel.config"

#include <strings.h>
#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <stdlib.h>
#include "sockets.h"
#include <commons/string.h>
#include <commons/collections/list.h>
#include "planificador.h"
#include "kernel.h"
#include <time.h>

int flag_ejecutar;

void init_logger();
void init_config();
char* leer_configuracion_para_string(char* parametro);
int leer_configuracion_para_int(char* parametro);
void terminar_programa();
void destruir_queue();
void opciones();
int analisis_instruccion(char*);
void* leer_consola();
void cargar_instruccion_en_cola_new(char*);
void cargar_la_struct_con_la_config();
void cargar_parametros_modificados();
void init_listas_de_mem_y_criterios();
void* consultar_memorias();
int largo_instruccion(char**);
int validar_numero(char*);
int validar_consistencia(char*);











#endif /* SRC_API_KERNEL_H_ */
