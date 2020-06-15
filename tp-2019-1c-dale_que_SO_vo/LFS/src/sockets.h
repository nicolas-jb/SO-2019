#ifndef SOCKETS_H_
#define SOCKETS_H_

#include "LFS.h"
#include <stdlib.h>
#include <limits.h>
#include <stdio.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/string.h>
#include <readline/readline.h>
#include <pthread.h>

// Variables globales
t_log* logger;

// Enum temporal para handshake (aun no implementado)
typedef enum {
	KERNEL, POOL, LFS
} Modulo;

//Estructura utilizada para el envio de datos
typedef enum {
	SELECT,
	INSERT,
	CREATE,
	DESCRIBE,
	DROP,
	JOURNAL,
	ADD,
	RUN,
	METRICS,
	MEMORIA,
	GOSSIPING,
	TAMANIO_VALUE,
	FINALIZAR,
	ERROR
} Instruccion;

typedef struct {
	int tamanio;
	void* stream;
} Buffer;

typedef struct {
	Instruccion instruccion;
	Buffer* buffer;
} Paquete;

//De consola
int validar_consistencia_consola(char* texto);
void opciones();
int largo_instruccion(char** instruccion);
int validar_numero(char* texto);

//Funciones disponibles
void iniciar_logger();

void leer_config();

int conectar(char*, char*, Modulo);
int escuchar(char*, char*);

void enviar_mensaje(int socket, char* mensaje);
void enviar(int, char*);
Paquete* crear_paquete();
void enviar_paquete(int, Paquete*);
Paquete* recibir_paquete(int socket);

void enviar_saludo(int, Modulo);
int recibir_saludo(int, Modulo);

Paquete* crear_paquete(Instruccion instruccion, char* mensaje);
void eliminar_paquete(Paquete* paquete);

void crear_hilo_por_cada_cliente(int, Modulo);

//SELECT
Paquete* select_lfs(Paquete* paquete);
//void select_lfs(char* nombre_tabla, uint16_t key); // ELIMINAR Test
//CREATE
Paquete* create(Paquete* paquete);
//DROP
Paquete* drop(Paquete* paquete);
//DESCRIBE
Paquete* describe(Paquete* paquete);
//INSERT
Paquete* insert(Paquete* paquete_peticion);

//MANEJO DE REQUESTS
void crear_hilo_por_cada_request(int socket, Modulo quien_soy);
void* atender_request(void* socket);
void* manejar_requests();

#endif
