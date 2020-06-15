/*
 * metricas.c
 *
 *  Created on: 15 jun. 2019
 *      Author: utnso
 */

#include "metricas.h"


void* mostrar_las_metricas(){
	int flag_correr_metricas = 0;
	while(1){
		pthread_mutex_lock(&mutex_ingreso_archivo_config);
		flag_correr_metricas = configuracion_kernel->run_metrics;
		pthread_mutex_unlock(&mutex_ingreso_archivo_config);

		sleep(30);
		if(flag_correr_metricas){
		//la función metrics debe recibir un script, por lo que creo uno auxiliar
		Script* script_auxiliar = malloc(sizeof(Script));
		//script_auxiliar = generar_script_una_linea(instruccion_auxiliar);
		funcion_metrics(script_auxiliar);
		free(script_auxiliar);
		}
	}

	//Protocolo porque nunca llega
	return NULL;
}
