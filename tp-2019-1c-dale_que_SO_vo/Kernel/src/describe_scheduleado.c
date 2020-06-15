/*
 * describe_scheduleado.c
 *
 *  Created on: 14 jul. 2019
 *      Author: utnso
 */

#include "planificador.h"

void* describe_scheduleado() {
	int socket_servidor;
	Paquete* respuesta;
	sem_wait(&s_espero_el_primer_describe);
	while (1) {
		pthread_mutex_lock(&mutex_ingreso_archivo_config);
		int retardo = configuracion_kernel->metadata_refresh / 1000;
		pthread_mutex_unlock(&mutex_ingreso_archivo_config);

		sleep(retardo);

		pthread_mutex_lock(&mutex_memorias);

		S_Memoria* memoria_a_utilizar;

		memoria_a_utilizar = obtener_memoria(NULL, NULL, 0);
		if (memoria_a_utilizar == NULL) {
			log_error(logger, "No dispongo de memorias para la instrucción DESCRIBE SCHEDULEADO");
			log_error(logger, "NO se actualizaron las metadata de las tablas");
		} else {
			socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);
			while (socket_servidor == -1 && memoria_a_utilizar != NULL) {

				log_error(logger, "La memoria %d a la que me iba a conectar está caída, se remueve de la lista de memorias conocidas", memoria_a_utilizar->id);

				if (conozco_la_memoria(criterio_SC, memoria_a_utilizar)) {
					remover_de_lista(criterio_SC, memoria_a_utilizar);
				}
				if (conozco_la_memoria(criterio_EC, memoria_a_utilizar)) {
					remover_de_lista(criterio_EC, memoria_a_utilizar);
				}
				if (conozco_la_memoria(criterio_SHC, memoria_a_utilizar)) {
					remover_de_lista(criterio_SHC, memoria_a_utilizar);
					//Como saco una memoria del criterio SHC actualizo el diccionario
					log_info(logger, "Se bajó una memoria asociada al SHC Consistency, por lo que se genera el diccionario nuevamente");
					dictionary_clean(diccionario_shc_memorias);
					generar_diccionario();
					log_info(logger, "Diccionario actualizado");
				}

				eliminar_de_lista_de_memorias(memoria_a_utilizar);

				log_info(logger, "Se busca otra memoria para ejecutar la instrucción DESCRIBE SCHEDULEADO");
				memoria_a_utilizar = obtener_memoria(NULL, NULL, 0);
				if (memoria_a_utilizar == NULL) {
					log_error(logger, "No dispongo de memorias para la instrucción DESCRIBE SCHEDULEADO");
				} else {

					socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);
				}

			}
			if (memoria_a_utilizar != NULL) {

				if (socket_servidor == -1) {
					log_error(logger, "No dispongo de memorias para la instrucción DESCRIBE SCHEDULEADO");
					log_error(logger, "NO se actualizaron las metadata de las tablas");
				} else {
					log_info(logger, "Se ejecuta el DESCRIBE SCHEDULEADO");
					log_info(logger, "Se envía instrucción al pool");
					//	log_info(logger, "Envio la instruccion DESCRIBE SCHEDULEADO al socket %d", socket_servidor); //todo ojo
					enviar(socket_servidor, "DESCRIBE");
					respuesta = recibir_paquete(socket_servidor);
					if (close(socket_servidor) == -1) {
						log_warning(logger, "Error al cerrar el socket en el describe scheduleado...");
					}
					log_info(logger, "Se obtuvo respuesta del pool:");
					if (string_starts_with(respuesta->buffer->stream, "ERROR")) {
						log_error(logger, "Falló la instrucción DESCRIBE SCHEDULEADO");
					} else {
						//Cada dato del describe se va a separar por un ; pero cada describe por un --
						char** respuesta_separada = string_split(respuesta->buffer->stream, "--");
						//Para identificar si es un describe de una tabla o global
						if (largo_instruccion(respuesta_separada) == 1) {
							cargar_datos_metadata(respuesta_separada[0]);
						} else {
							for (int i = 0; respuesta_separada[i] != NULL; i++) {
								cargar_datos_metadata(respuesta_separada[i]);
							}
						}
						free(respuesta_separada);
					}
					eliminar_paquete(respuesta);
				}
			}

		}

		pthread_mutex_unlock(&mutex_memorias);
	}

	//Protocolar
	return NULL;
}
