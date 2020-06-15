#ifndef SOCKETS_H_
#define SOCKETS_H_

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
#include <readline/readline.h>
#include <pthread.h>
#include <semaphore.h>
#include "kernel.h"
#include "string.h"
#include <ctype.h>
#include <stdarg.h>



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
} Operacion;

typedef struct {
	int tamanio;
	void* stream;
} Buffer;

typedef struct {
	Operacion instruccion;
	Buffer* buffer;
} Paquete;

void enviar(int, char*);
void enviar_paquete(int, Paquete*);

int conectar(char*, char*, Modulo);
int escuchar(char*, char*);

Paquete* recibir(int socket);
Paquete* crear_paquete(Operacion instruccion, char* mensaje);
void eliminar_paquete(Paquete* paquete);

void enviar_saludo(int, Modulo);
int recibir_saludo(int, Modulo);
Paquete* recibir_paquete(int);




#endif
