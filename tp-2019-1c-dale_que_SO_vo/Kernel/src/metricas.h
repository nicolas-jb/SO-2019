/*
 * metricas.h
 *
 *  Created on: 15 jun. 2019
 *      Author: utnso
 */

#ifndef SRC_METRICAS_H_
#define SRC_METRICAS_H_

#include <stdlib.h>
#include <stdio.h>
#include <commons/log.h>
#include "kernel.h"
#include "API-Kernel.h"
#include "planificador.h"
#include <pthread.h> // -lpthread
#include "sockets.h"
#include <semaphore.h>
#include <sys/types.h>
#include <commons/string.h>
#include <string.h>
#include <time.h>


void* mostrar_las_metricas();

#endif /* SRC_METRICAS_H_ */
