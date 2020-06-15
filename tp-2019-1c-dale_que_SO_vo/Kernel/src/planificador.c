/*
 * planificador.c
 *
 *  Created on: 7 jun. 2019
 *      Author: utnso
 */

#include "planificador.h"

void init_estados_panif() {
	cola_new = queue_create();
	cola_ready = queue_create();
	cola_exit = queue_create();
	cola_execute = queue_create();
}

Script* generar_script_una_linea(Instruccion* instruccion) {
	Script* nuevo_script = malloc(sizeof(Script));
	pthread_mutex_lock(&mutex_id_script);
	nuevo_script->id_script = id_scripts;
	id_scripts++;
	pthread_mutex_unlock(&mutex_id_script);
	nuevo_script->cod_instruccion = instruccion->cod_instruccion;
	nuevo_script->datos = instruccion->datos;
	nuevo_script->tipo_script = CONSOLA;
	nuevo_script->path = "";
	nuevo_script->lineas_pendientes = 1;
	nuevo_script->cantidad_de_lineas = nuevo_script->lineas_pendientes; //ni bien genero el script coinciden
	nuevo_script->estado_script = PENDIENTE;

	return nuevo_script;
}

Script* generar_script_path(Instruccion* instruccion) {
	Script* nuevo_script = malloc(sizeof(Script));

	pthread_mutex_lock(&mutex_id_script);
	nuevo_script->id_script = id_scripts;
	id_scripts++;
	pthread_mutex_unlock(&mutex_id_script);
	nuevo_script->cod_instruccion = instruccion->cod_instruccion;
	nuevo_script->datos = instruccion->datos;
	nuevo_script->tipo_script = ARCHIVO;
	nuevo_script->path = nuevo_script->datos; //ni bien lo genero el path coincide con los datos
	nuevo_script->cantidad_de_lineas = cantidad_lineas(nuevo_script->path);
	nuevo_script->lineas_pendientes = nuevo_script->cantidad_de_lineas; //ni bien genero el script coinciden
	nuevo_script->estado_script = PENDIENTE;

	return nuevo_script;
}

char* instruccion_del_archivo_segun_linea(int nro_de_linea, char* path) {
	FILE *fp;
	fp = fopen(path, "r");
	if (fp == NULL) {
		log_error(logger, "Error al abrir el archivo");
	}

	char buffer[128];
	//char *line_buf = NULL;
	//size_t line_buf_size = 0;
	int line_count = 0;
	while (line_count <= nro_de_linea) {
		line_count++;
		fgets(buffer, sizeof(buffer), fp);
		//getline(&line_buf, &line_buf_size, fp);
	}
	fclose(fp);
	//char* rta = &buffer[0];
	char* rta = string_new();
	string_append(&rta, buffer);
	return rta;
}

int cantidad_lineas(char* path) {
	FILE *fp;
	fp = fopen(path, "r");
	int cantidad_lineas = 0;
	char caracter, caracter_anterior = '\n';
	if (fp == NULL) {
		log_error(logger, "Error al abrir el archivo");
	}
	fseek(fp, 0, SEEK_SET);
	while ((caracter = fgetc(fp)) != EOF) {
		if (caracter_anterior == '\n') {
			cantidad_lineas++;
		}
		caracter_anterior = caracter;
	}
	fclose(fp);
	return cantidad_lineas;
}

int minimo_entre(int a, int b) {
	if (a > b) {
		return b;
	} else {
		return a;
	}
}

void* pasar_a_ready() {
	while (1) {

		sem_wait(&s_hay_elementos_en_new);

		pthread_mutex_lock(&mutex_carrera);
		Instruccion* instruccion = queue_pop(cola_new);
		pthread_mutex_unlock(&mutex_carrera);
		Script* nuevo_script = malloc(sizeof(Script));

		if (string_starts_with(instruccion->cod_instruccion, "RUN")) {
			nuevo_script = generar_script_path(instruccion);
		} else {
			nuevo_script = generar_script_una_linea(instruccion);
		}
		log_info(logger, "Se generó un script de id = %d debido a la instrucción %s", nuevo_script->id_script, nuevo_script->cod_instruccion);
		log_info(logger, "El script de id = %d ingresó a la cola de Ready ", nuevo_script->id_script);
		pthread_mutex_lock(&mutex_carrera);
		queue_push(cola_ready, nuevo_script);
		pthread_mutex_unlock(&mutex_carrera);

		sem_post(&s_hay_elementos_en_ready);

	}

	return NULL;
}

void* ejecutar() {
	int grado_multiprocesamiento;
	int quantum;
	//int retardo;

	while (1) {

		sem_wait(&s_hay_elementos_en_ready);

		pthread_mutex_lock(&mutex_ingreso_archivo_config);
		grado_multiprocesamiento = configuracion_kernel->multiprocesamiento;
		quantum = configuracion_kernel->quantum;
		pthread_mutex_unlock(&mutex_ingreso_archivo_config);

		/*Este if es para ir creando a medida que se van ejecutando los script,
		 * de esta forma no tengo que esperar a que se terminen de ejecutar todos los hilso sino
		 * que a medida que van terminando voy creando.
		 * Para esto tengo en cuenta el grado de multip y los hilos que aún están ejecutando
		 */
		pthread_mutex_lock(&mutex_exec);
		int resultado = cantidad_de_hilos_en_exec < grado_multiprocesamiento;
		pthread_mutex_unlock(&mutex_exec);
		if (resultado) {
			/*Tengo que crear hilos dinámicos, para esto veo el mínimo entre el tamaño de la cola ready
			 *y la diferencia entre los hilos que ya cree y el grado de multip actual
			 */
			pthread_mutex_lock(&mutex_carrera);
			Script* un_script = queue_pop(cola_ready);
			pthread_mutex_unlock(&mutex_carrera);
			log_info(logger, "El script de id = %d ingresó a la cola de Exec ", un_script->id_script);
			pthread_mutex_lock(&mutex_carrera);
			queue_push(cola_execute, un_script);
			crear_hilos_de_ejecucion(minimo_entre((grado_multiprocesamiento - cantidad_de_hilos_en_exec), queue_size(cola_execute)), quantum);
			pthread_mutex_unlock(&mutex_carrera);
		} else {

			sem_wait(&s_se_termino_un_exec);

			sem_post(&s_hay_elementos_en_ready);
		}

	}

	//Protocolar, nunca llegará
	return NULL;
}

void crear_hilos_de_ejecucion(int cantidad, int quantum) {

	pthread_t hilo_exec;
	for (int i = 0; i < cantidad; i++) {

		pthread_mutex_lock(&mutex_exec);
		cantidad_de_hilos_en_exec++;
		pthread_mutex_unlock(&mutex_exec);
		pthread_create(&hilo_exec, NULL, hilos_execute, (void*) quantum);
		pthread_detach(hilo_exec);

	}

}

void* hilos_execute(void* quantum) {

	int my_quantum = (int) quantum;

	pthread_mutex_lock(&mutex_exec);
	Script* un_script = queue_pop(cola_execute);
	pthread_mutex_unlock(&mutex_exec);

	ejecutar_las_request(un_script, my_quantum);

	pthread_mutex_lock(&mutex_exec);
	cantidad_de_hilos_en_exec--;
	sem_post(&s_se_termino_un_exec);
	pthread_mutex_unlock(&mutex_exec);

	return NULL;
}

void ejecutar_las_request(Script* un_script, int quantum) {

	if (instruccion_a_pool(un_script->cod_instruccion)) {
		funcion_del_pool(un_script);
	} else if (string_starts_with(un_script->cod_instruccion, "RUN")) {
		funcion_run(un_script, quantum);
	} else if (string_starts_with(un_script->cod_instruccion, "ADD")) {
		funcion_add(un_script);
	} else if (string_starts_with(un_script->cod_instruccion, "JOURNAL")) {
		funcion_journal(un_script);
	} else if (string_starts_with(un_script->cod_instruccion, "METRICS")) {
		funcion_metrics(un_script);
	} else {
		log_error(logger, "VAYA POR DIOS QUE LAMENTABLE ACCIDENTE by Hollywood Monsters ");
		log_error(logger, "No mentira, las instrucciones van en mayúscula");
	}

	if (!(un_script->tipo_script == ARCHIVO)) {
		if (un_script->estado_script == TERMINADO_SIN_ERROR) {
			log_info(logger, "El script de id = %d ha terminado correctamente y se pasa a la cola de exit", un_script->id_script);
			pthread_mutex_lock(&mutex_carrera);
			queue_push(cola_exit, un_script);
			pthread_mutex_unlock(&mutex_carrera);

		} else {
			log_info(logger, "Se ha detectado un error en el script de id = %d, por lo que se deja de ejecutar y se pasa a la cola de exit", un_script->id_script);
			pthread_mutex_lock(&mutex_carrera);
			queue_push(cola_exit, un_script);
			pthread_mutex_unlock(&mutex_carrera);

		}
	}

}

int instruccion_a_pool(char* cod_instruccion) {

	int respuesta;

	if (string_starts_with(cod_instruccion, "SELECT")) {
		respuesta = 1;
	} else if (string_starts_with(cod_instruccion, "INSERT")) {
		respuesta = 1;
	} else if (string_starts_with(cod_instruccion, "CREATE")) {
		respuesta = 1;
	} else if (string_starts_with(cod_instruccion, "DESCRIBE")) {
		respuesta = 1;
	} else if (string_starts_with(cod_instruccion, "DROP")) {
		respuesta = 1;
	} else {
		respuesta = 0;
	}

	return respuesta;
}
void* obtener_memoria(char* nombre_tabla, char* valor_key, int modo) {
	log_info(logger, "Estoy obteniendo una memoria para ejecutar");
	S_Memoria* memoria_a_utilizar;
	if (modo) {
		log_info(logger, "Instruccion Hasheable");
		if (string_equals_ignore_case(obtener_consistencia_la_tabla(nombre_tabla), "SC")) {
			if (list_size(criterio_SC) == 0) {
				log_error(logger, "La metadata de la tabla tiene consistencia SC y no se dispone ninguna memoria asociada a dicho criterio");
				return NULL;
			}
			memoria_a_utilizar = list_get(criterio_SC, 0);

		} else if (string_equals_ignore_case(obtener_consistencia_la_tabla(nombre_tabla), "SHC")) {
			if (list_size(criterio_SHC) == 0) {
				log_error(logger, "La metadata de la tabla tiene consistencia SHC y no se dispone ninguna memoria asociada a dicho criterio");
				return NULL;
			}
			int key = atoi(valor_key);
			memoria_a_utilizar = dictionary_get(diccionario_shc_memorias, string_itoa((key % dictionary_size(diccionario_shc_memorias))));
		} else if (string_equals_ignore_case(obtener_consistencia_la_tabla(nombre_tabla), "EC")) {
			if (list_size(criterio_EC) == 0) {
				log_error(logger, "La metadata de la tabla tiene consistencia EC y no se dispone ninguna memoria asociada a dicho criterio");
				return NULL;
			}
			memoria_a_utilizar = list_get(criterio_EC, rand_r(&semilla) % list_size(criterio_EC));
		} else {
			log_error(logger, "No se dispone de le metadata de la tabla");
			return NULL;
		}
	} else {
		//Para las instrucciones no hasheables eligio una memoria con consistencia eventual aleatoria, si tengo asignadas a ese criterio y sino sigo buscando
		log_info(logger, "Instruccion NO Hasheable");
		if (list_size(criterio_EC) > 0) {
			memoria_a_utilizar = list_get(criterio_EC, rand_r(&semilla) % list_size(criterio_EC));
		} else if (list_size(criterio_SHC) > 0) {
			memoria_a_utilizar = list_get(criterio_SHC, rand_r(&semilla) % list_size(criterio_SHC));
		} else if (list_size(criterio_SC) > 0) {
			memoria_a_utilizar = list_get(criterio_SC, 0);
		} else {
			log_error(logger, "No se dispone de memorias");
			return NULL;
		}

	}
	log_info(logger, "Obtuve memoria para operar, id_memoria = %d", memoria_a_utilizar->id);
	return memoria_a_utilizar;
}

void funcion_del_pool(Script* un_script) {
	char* instruccion_para_pool = string_new();
	int socket_servidor;
	Paquete* respuesta;

	pthread_mutex_lock(&mutex_ingreso_archivo_config);
	int retardo = configuracion_kernel->sleep_ejecucion / 1000;
	pthread_mutex_unlock(&mutex_ingreso_archivo_config);

	string_append(&instruccion_para_pool, un_script->cod_instruccion);
	if (un_script->datos != NULL) {
		string_append(&instruccion_para_pool, " ");
		string_append(&instruccion_para_pool, un_script->datos);
	}

	S_Memoria* memoria_a_utilizar;

	pthread_mutex_lock(&mutex_memorias);

	if (string_equals_ignore_case(un_script->cod_instruccion, "SELECT")) {
		un_script->estado_script = TERMINADO_SIN_ERROR;
		char** tabla = string_n_split(un_script->datos, 2, " ");
		memoria_a_utilizar = obtener_memoria(tabla[0], tabla[1], 1);

		if (memoria_a_utilizar == NULL) {
			un_script->estado_script = TERMINADO_CON_ERROR;
			flag_eliminar_paquete = 0;
		}

		if (un_script->estado_script != TERMINADO_CON_ERROR) {

			socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);

			//Si la consistencia es eventual, busco alguna memoria asociada a ese criterio que no se haya caído
			if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "EC") && socket_servidor == -1) {
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

					log_info(logger, "Se busca otra memoria de consistencia evuentual para ejecutar la instrucción SELECT");
					memoria_a_utilizar = obtener_memoria(tabla[0], tabla[1], 1);
					if (memoria_a_utilizar != NULL) {
						socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);
					}

				}
				if (memoria_a_utilizar == NULL) {
					log_error(logger, "No dispongo de memorias para la instrucción SELECT");
					un_script->estado_script = TERMINADO_CON_ERROR;
					flag_eliminar_paquete = 0;
				}
			}
			if (un_script->estado_script != TERMINADO_CON_ERROR) {

				if (socket_servidor == -1) {

					un_script->estado_script = TERMINADO_CON_ERROR;
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
					flag_eliminar_paquete = 0;

				} else {
					pthread_mutex_lock(&mutex_metricas);
					if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "SC")) {
						time(&SC->t0_select);
					} else if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "SHC")) {
						time(&SHC->t0_select);
					} else if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "EC")) {
						time(&EC->t0_select);
					}

					log_info(logger, "Se envía instrucción al pool");

					//	log_info(logger, "Envio la instruccion %s al socket %d", instruccion_para_pool, socket_servidor); //todo ojo

					enviar(socket_servidor, instruccion_para_pool);

					respuesta = recibir_paquete(socket_servidor);
					if (close(socket_servidor) == -1) {
						log_warning(logger, "Error al cerrar el socket de la función select...");
					}
					log_info(logger, "Se obtuvo respuesta del pool:");
					//log_info(logger, "Respuesta sin validar:");//todo
					//log_info(logger, respuesta->buffer->stream);//todo

//					if ((respuesta->instruccion == ERROR) && (string_starts_with(respuesta->buffer->stream, "FULL"))) {
//						eliminar_paquete(respuesta);
//						log_warning(logger, "No se puede ejecutar la instrucción SELECT por tener la memoria en estado FULL");
//						log_info(logger, "Se ejecuta la instrucción JOURNAL a la memoria involucrada => %d", memoria_a_utilizar->id);
//						socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);
//						//		log_info(logger, "Envio la instruccion journal forzoso al socket %d", socket_servidor); //todo ojo
//						enviar(socket_servidor, "JOURNAL");
//						respuesta = recibir_paquete(socket_servidor);
//						if (close(socket_servidor) == -1) {
//							log_warning(logger, "Error al cerrar el socket de la función select....");
//						}
//						log_info(logger, respuesta->buffer->stream);
//						eliminar_paquete(respuesta);
//						log_info(logger, "Vuelvo a mandar la instrucción SELECT");
//						socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);
//						//	log_info(logger, "Envio la instruccion %s al socket %d", instruccion_para_pool, socket_servidor); //todo ojo
//						enviar(socket_servidor, instruccion_para_pool);
//						respuesta = recibir_paquete(socket_servidor);
//						if (close(socket_servidor) == -1) {
//							log_warning(logger, "Error al cerrar el socket de la función select.");
//						}
//						log_info(logger, "Se obtuvo respuesta del pool:");
//					}

					if (respuesta->instruccion == ERROR) {
						log_error(logger, "Falló la instrucción SELECT");
						log_error(logger, respuesta->buffer->stream);
						un_script->estado_script = TERMINADO_SIN_ERROR;
					} else {

						if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "SC")) {
							time(&SC->tf_select);
							SC->cantidad_selects += 1;
							SC->duracion_select += difftime(SC->tf_select, SC->t0_select);
						} else if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "SHC")) {
							time(&SHC->tf_select);
							SHC->cantidad_selects += 1;
							SHC->duracion_select += difftime(SHC->tf_select, SHC->t0_select);
						} else if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "EC")) {
							time(&EC->tf_select);
							EC->cantidad_selects += 1;
							EC->duracion_select += difftime(EC->tf_select, EC->t0_select);
						}

						log_info(logger, "%s", respuesta->buffer->stream);
						un_script->estado_script = TERMINADO_SIN_ERROR;
					}
					eliminar_paquete(respuesta);
					pthread_mutex_unlock(&mutex_metricas);
				}
			}
		}

		free(tabla[0]);
		free(tabla[1]);
		free(tabla);
		sleep(retardo);
	} else if (string_equals_ignore_case(un_script->cod_instruccion, "INSERT")) {
		un_script->estado_script = TERMINADO_SIN_ERROR;
		char** tabla = string_n_split(un_script->datos, 3, " ");
		memoria_a_utilizar = obtener_memoria(tabla[0], tabla[1], 1);
		if (memoria_a_utilizar == NULL) {
			un_script->estado_script = TERMINADO_CON_ERROR;
			flag_eliminar_paquete = 0;
		}

		if (un_script->estado_script != TERMINADO_CON_ERROR) {

			socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);

			//Si la consistencia es eventual, busco alguna memoria asociada a ese criterio que no se haya caído
			if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "EC") && socket_servidor == -1) {
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

					log_info(logger, "Se busca otra memoria de consistencia evuentual para ejecutar la instrucción INSERT");
					memoria_a_utilizar = obtener_memoria(tabla[0], tabla[1], 1);
					if (memoria_a_utilizar != NULL) {
						socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);
					}
				}
				if (memoria_a_utilizar == NULL) {
					log_error(logger, "No dispongo de memorias para la instrucción INSERT");
					un_script->estado_script = TERMINADO_CON_ERROR;
					flag_eliminar_paquete = 0;
				}
			}
			if (un_script->estado_script != TERMINADO_CON_ERROR) {

				if (socket_servidor == -1) {
					un_script->estado_script = TERMINADO_CON_ERROR;
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
					flag_eliminar_paquete = 0;

				} else {
					pthread_mutex_lock(&mutex_metricas);
					if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "SC")) {
						time(&SC->t0_insert);
					} else if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "SHC")) {
						time(&SHC->t0_insert);
					} else if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "EC")) {
						time(&EC->t0_insert);
					}

					log_info(logger, "Se envía instrucción al pool");
					//	log_info(logger, "Envio la instruccion %s al socket %d", instruccion_para_pool, socket_servidor); //todo ojo
					enviar(socket_servidor, instruccion_para_pool);

					respuesta = recibir_paquete(socket_servidor);
					//log_info(logger, "Respuesta sin validar:");//todo
					//log_info(logger, respuesta->buffer->stream);//todo

					if (close(socket_servidor) == -1) {
						log_warning(logger, "Error al cerrar el socket de la función insert...");
					}
					log_info(logger, "Se obtuvo respuesta del pool:");

//					if ((respuesta->instruccion == ERROR) && (string_starts_with(respuesta->buffer->stream, "FULL"))) {
//						eliminar_paquete(respuesta);
//						log_warning(logger, "No se puede ejecutar la instrucción INSERT por tener la memoria en estado FULL");
//						log_info(logger, "Se ejecuta la instrucción JOURNAL a la memoria involucrada => %d", memoria_a_utilizar->id);
//						socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);
//						//			log_info(logger, "Envio la instruccion journal forzoso al socket %d", socket_servidor); //todo ojo
//						enviar(socket_servidor, "JOURNAL");
//						respuesta = recibir_paquete(socket_servidor);
//						if (close(socket_servidor) == -1) {
//							log_warning(logger, "Error al cerrar el socket de la función insert....");
//						}
//						log_info(logger, respuesta->buffer->stream);
//						eliminar_paquete(respuesta);
//						log_info(logger, "Vuelvo a mandar la instrucción INSERT");
//						socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);
//						//			log_info(logger, "Envio la instruccion %s al socket %d", instruccion_para_pool, socket_servidor); //todo ojo
//						enviar(socket_servidor, instruccion_para_pool);
//						respuesta = recibir_paquete(socket_servidor);
//						if (close(socket_servidor) == -1) {
//							log_warning(logger, "Error al cerrar el socket de la función insert.....");
//						}
//						log_info(logger, "Se obtuvo respuesta del pool:");
//					}

					if (respuesta->instruccion == ERROR) {
						log_error(logger, "Falló la instrucción INSERT");
						log_error(logger, respuesta->buffer->stream);
						un_script->estado_script = TERMINADO_SIN_ERROR;
					} else {

						if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "SC")) {
							time(&SC->tf_insert);
							SC->cantidad_inserts += 1;
							SC->duracion_insert += difftime(SC->tf_insert, SC->t0_insert);
						} else if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "SHC")) {
							time(&SHC->tf_insert);
							SHC->cantidad_inserts += 1;
							SHC->duracion_insert += difftime(SHC->tf_insert, SHC->t0_insert);
						} else if (string_equals_ignore_case(obtener_consistencia_la_tabla(tabla[0]), "EC")) {
							time(&EC->tf_insert);
							EC->cantidad_inserts += 1;
							EC->duracion_insert += difftime(EC->tf_insert, EC->t0_insert);
						}

						log_info(logger, "%s", respuesta->buffer->stream);
						un_script->estado_script = TERMINADO_SIN_ERROR;
					}
					eliminar_paquete(respuesta);
					pthread_mutex_unlock(&mutex_metricas);
				}

			}
		}

		free(tabla[0]);
		free(tabla[1]);
		free(tabla[2]);
		free(tabla);
		sleep(retardo);
	} else if (string_equals_ignore_case(un_script->cod_instruccion, "CREATE")) {
		un_script->estado_script = TERMINADO_SIN_ERROR;
		memoria_a_utilizar = obtener_memoria(NULL, NULL, 0);
		if (memoria_a_utilizar == NULL) {
			log_error(logger, "No dispongo de memorias para la instrucción CREATE");
			un_script->estado_script = TERMINADO_CON_ERROR;
			flag_eliminar_paquete = 0;
		}

		if (un_script->estado_script != TERMINADO_CON_ERROR) {
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

				log_info(logger, "Se busca otra memoria para ejecutar la instrucción CREATE");
				memoria_a_utilizar = obtener_memoria(NULL, NULL, 0);
				if (memoria_a_utilizar != NULL) {
					socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);
				}
			}
			if (memoria_a_utilizar == NULL) {
				log_error(logger, "No dispongo de memorias para la instrucción CREATE");
				un_script->estado_script = TERMINADO_CON_ERROR;
				flag_eliminar_paquete = 0;
			} else {

				log_info(logger, "Se envía instrucción al pool");
				//		log_info(logger, "Envio la instruccion %s al socket %d", instruccion_para_pool, socket_servidor); //todo ojo
				enviar(socket_servidor, instruccion_para_pool);
				respuesta = recibir_paquete(socket_servidor);
				if (close(socket_servidor) == -1) {
					log_warning(logger, "Error al cerrar el socket de la función create..");
				}
				log_info(logger, "Se obtuvo respuesta del pool:");

				if (respuesta->instruccion == ERROR) {
					log_error(logger, "Falló la instrucción CREATE");
					log_error(logger, respuesta->buffer->stream);
					eliminar_paquete(respuesta);
					un_script->estado_script = TERMINADO_CON_ERROR;
				} else {
					log_info(logger, "%s", respuesta->buffer->stream);
					eliminar_paquete(respuesta);
					un_script->estado_script = TERMINADO_SIN_ERROR;
					char** tabla = string_n_split(un_script->datos, 4, " ");
					char* metadata = string_new();
					string_append(&metadata, tabla[0]);
					string_append(&metadata, ";");
					string_append(&metadata, tabla[1]);
					string_append(&metadata, ";");
					string_append(&metadata, tabla[2]);
					string_append(&metadata, ";");
					string_append(&metadata, tabla[3]);

					cargar_datos_metadata(metadata);
					free(tabla[0]);
					free(tabla[1]);
					free(tabla[2]);
					free(tabla[3]);
					free(tabla);

				}
			}

		}
		sleep(retardo);
	} else if (string_equals_ignore_case(un_script->cod_instruccion, "DESCRIBE")) {
		un_script->estado_script = TERMINADO_SIN_ERROR;
		memoria_a_utilizar = obtener_memoria(NULL, NULL, 0);
		if (memoria_a_utilizar == NULL) {
			log_error(logger, "No dispongo de memorias para la instrucción DESCRIBE");
			un_script->estado_script = TERMINADO_CON_ERROR;
			flag_eliminar_paquete = 0;
		}

		if (un_script->estado_script != TERMINADO_CON_ERROR) {
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

				log_info(logger, "Se busca otra memoria para ejecutar la instrucción DESCRIBE");
				memoria_a_utilizar = obtener_memoria(NULL, NULL, 0);
				if (memoria_a_utilizar != NULL) {
					socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);
				}

			}
			if (memoria_a_utilizar == NULL) {
				log_error(logger, "No dispongo de memorias para la instrucción DESCRIBE");
				un_script->estado_script = TERMINADO_CON_ERROR;
				flag_eliminar_paquete = 0;
			} else {
				//socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);
				log_info(logger, "Se envía instrucción al pool");
				//	log_info(logger, "Envio la instruccion %s al socket %d", instruccion_para_pool, socket_servidor); //todo ojo
				enviar(socket_servidor, instruccion_para_pool);
				respuesta = recibir_paquete(socket_servidor);
				if (close(socket_servidor) == -1) {
					log_warning(logger, "Error al cerrar el socket de la función describe..");
				}
				log_info(logger, "Se obtuvo respuesta del pool:");

				if (respuesta->instruccion == ERROR) {
					log_error(logger, "Falló la instrucción DESCRIBE");
					log_error(logger, respuesta->buffer->stream);
					eliminar_paquete(respuesta);
					un_script->estado_script = TERMINADO_CON_ERROR;
				} else {
					//Cada dato del describe se va a separar por un ; pero cada describe por un --
					char** respuesta_separada = string_split(respuesta->buffer->stream, "--");
					eliminar_paquete(respuesta);
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
			}

		}

		if (flag_primer_describe && (un_script->estado_script == TERMINADO_SIN_ERROR)) {
			flag_primer_describe = 0;
			sem_post(&s_espero_el_primer_describe);
		}
		sleep(retardo);
	} else if (string_equals_ignore_case(un_script->cod_instruccion, "DROP")) {
		un_script->estado_script = TERMINADO_SIN_ERROR;
		if (string_equals_ignore_case("NO CONOZCO TABLA", obtener_consistencia_la_tabla(un_script->datos))) {
			log_error(logger, "No tengo conocimiento de la tabla, NO puedo ejecutar DROP ");
			un_script->estado_script = TERMINADO_CON_ERROR;
			flag_eliminar_paquete = 0;
		} else {
			memoria_a_utilizar = obtener_memoria(NULL, NULL, 0);
			if (memoria_a_utilizar == NULL) {
				log_error(logger, "No dispongo de memorias para la instrucción DROP");
				un_script->estado_script = TERMINADO_CON_ERROR;
				flag_eliminar_paquete = 0;
			}

			if (un_script->estado_script != TERMINADO_CON_ERROR) {
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

					log_info(logger, "Se busca otra memoria para ejecutar la instrucción DROP");
					memoria_a_utilizar = obtener_memoria(NULL, NULL, 0);
					if (memoria_a_utilizar != NULL) {
						socket_servidor = conectar(memoria_a_utilizar->ip_m, memoria_a_utilizar->puerto_m, KERNEL);
					}

				}
				if (memoria_a_utilizar == NULL) {
					log_error(logger, "No dispongo de memorias para la instrucción DROP");
					un_script->estado_script = TERMINADO_CON_ERROR;
					flag_eliminar_paquete = 0;
				} else {
					log_info(logger, "Se envía instrucción al pool");
					//	log_info(logger, "Envio la instruccion %s al socket %d", instruccion_para_pool, socket_servidor); //todo ojo
					enviar(socket_servidor, instruccion_para_pool);
					respuesta = recibir_paquete(socket_servidor);
					if (close(socket_servidor) == -1) {
						log_warning(logger, "Error al cerrar el socket de la función drop..");
					}
					log_info(logger, "Se obtuvo respuesta del pool:");
					if (respuesta->instruccion == ERROR) {
						log_error(logger, "Falló la instrucción DROP");
						log_error(logger, respuesta->buffer->stream);
						eliminar_paquete(respuesta);
						un_script->estado_script = TERMINADO_CON_ERROR;
					} else {
						log_info(logger, "%s", respuesta->buffer->stream);
						eliminar_paquete(respuesta);
						un_script->estado_script = TERMINADO_SIN_ERROR;
					}
				}
			}

		}
		sleep(retardo);

	}

	free(instruccion_para_pool);
//	if (flag_eliminar_paquete) {
//		eliminar_paquete(respuesta);
//	} else {
//		flag_eliminar_paquete = 1;
//	}

	pthread_mutex_unlock(&mutex_memorias);

}

char* obtener_consistencia_la_tabla(char* tabla) {
	pthread_mutex_lock(&mutex_metadata);
	for (int i = 0; list_get(metadata, i) != NULL; i++) {
		S_Metadata* aux = list_get(metadata, i);
		if (string_equals_ignore_case(aux->nombre_tabla, tabla)) {
			pthread_mutex_unlock(&mutex_metadata);
			return aux->tipo_consistencia;
		}
	}

	pthread_mutex_unlock(&mutex_metadata);
	return "NO CONOZCO TABLA";

}

void cargar_datos_metadata(char* datos_tabla) {

	pthread_mutex_lock(&mutex_metadata);
	S_Metadata* aux;
	char** auxiliar = string_split(datos_tabla, ";");

	for (int i = 0; list_get(metadata, i) != NULL; i++) {
		aux = list_get(metadata, i);
		if (string_equals_ignore_case(aux->nombre_tabla, auxiliar[0])) {
			memset(aux->tipo_consistencia, 0, strlen(aux->tipo_consistencia));
			memset(aux->particiones, 0, strlen(aux->particiones));
			memset(aux->tiempo_compactacion, 0, strlen(aux->tiempo_compactacion));
			string_append(&aux->tipo_consistencia, auxiliar[1]);
			string_append(&aux->particiones, auxiliar[2]);
			string_append(&aux->tiempo_compactacion, auxiliar[3]);
			log_info(logger, "DATOS DEL DESCRIBE");
			log_info(logger, "%s", aux->nombre_tabla);
			log_info(logger, "%s", aux->tipo_consistencia);
			log_info(logger, "%s", aux->particiones);
			log_info(logger, "%s", aux->tiempo_compactacion);
			for (int j = 0; auxiliar[j] != NULL; j++) {
				free(auxiliar[j]);
			}

			free(auxiliar);
			free(datos_tabla);
			pthread_mutex_unlock(&mutex_metadata);
			return;
		}
	}
	S_Metadata* una_tabla = malloc(sizeof(S_Metadata));
	una_tabla->nombre_tabla = string_new();
	una_tabla->tipo_consistencia = string_new();
	una_tabla->particiones = string_new();
	una_tabla->tiempo_compactacion = string_new();

	string_append(&una_tabla->nombre_tabla, auxiliar[0]);
	string_append(&una_tabla->tipo_consistencia, auxiliar[1]);
	string_append(&una_tabla->particiones, auxiliar[2]);
	string_append(&una_tabla->tiempo_compactacion, auxiliar[3]);

	log_info(logger, "DATOS DEL DESCRIBE");
	log_info(logger, "%s", una_tabla->nombre_tabla);
	log_info(logger, "%s", una_tabla->tipo_consistencia);
	log_info(logger, "%s", una_tabla->particiones);
	log_info(logger, "%s", una_tabla->tiempo_compactacion);

	list_add(metadata, una_tabla);

	for (int i = 0; auxiliar[i] != NULL; i++) {
		free(auxiliar[i]);
	}

	free(auxiliar);
	free(datos_tabla);
	pthread_mutex_unlock(&mutex_metadata);
}
void funcion_journal(Script* script) {

	pthread_mutex_lock(&mutex_ingreso_archivo_config);
	int retardo = configuracion_kernel->sleep_ejecucion / 1000;
	pthread_mutex_unlock(&mutex_ingreso_archivo_config);

	log_info(logger, "Ejecuto journal");
	if (!list_size(criterio_SC)) {
		log_info(logger, "No hay memorias asociadas al Strong Consistency para hacer el Journal");
	} else {
		log_info(logger, "Comienza el Journal para las memorias asociadas al Strong Consistency");
		journal(criterio_SC);
	}

	if (!list_size(criterio_SHC)) {
		log_info(logger, "No hay memorias asociadas al Strong Hash Consistency para hacer el Journal");
	} else {
		log_info(logger, "Comienza el Journal para las memorias asociadas al Strong Hash Consistency");
		journal(criterio_SHC);
	}

	if (!list_size(criterio_EC)) {
		log_info(logger, "No hay memorias asociadas al Eventual Consistency para hacer el Journal");
	} else {
		log_info(logger, "Comienza el Journal para las memorias asociadas al Eventual Consistency");
		journal(criterio_EC);
	}
	log_info(logger, "Se termina la función Journal");
	script->estado_script = TERMINADO_SIN_ERROR;
	sleep(retardo);
}

void funcion_add(Script* script) {

	pthread_mutex_lock(&mutex_ingreso_archivo_config);
	int retardo = configuracion_kernel->sleep_ejecucion / 1000;
	pthread_mutex_unlock(&mutex_ingreso_archivo_config);

	char** instruccion = string_split(script->datos, " ");
	S_Memoria* una_memoria = malloc(sizeof(S_Memoria));
	una_memoria->id = atoi(instruccion[1]);

//Semáforo para evitar que la lectura se intersecte con la escritura del otro hilo (el que consulta al pool)
	pthread_mutex_lock(&mutex_lectura_memorias);

	if (conozco_la_memoria(memorias, una_memoria)) {
		una_memoria = buscar_memoria_por_id(una_memoria);
		script->estado_script = TERMINADO_SIN_ERROR;

		if (string_starts_with(instruccion[3], "SC")) {

			if (list_is_empty(criterio_SC)) {

				list_add(criterio_SC, una_memoria);
				log_info(logger, "Se asocia la memoria id = %d al criterio Strong Consistency", una_memoria->id);

			} else {
				S_Memoria* la_memoria_SC = list_get(criterio_SC, 0);
				if (la_memoria_SC->id == una_memoria->id) {
					log_warning(logger, "La memoria ya estaba bajo el criterio Strong Consistency");

				} else if (conectar(la_memoria_SC->ip_m, la_memoria_SC->puerto_m, KERNEL) == -1) {
					log_warning(logger, "La memoria que se tenía asignada a SC se cayó");
					log_info(logger, "Se asocia la memoria id = %d al criterio Strong Consistency", una_memoria->id);
					remover_de_lista(criterio_SC, la_memoria_SC);
					free(la_memoria_SC);
					list_add(criterio_SC, una_memoria);

				} else {
					log_error(logger, "Ya hay una memoria asociada al criterio Strong Consistency");
					script->estado_script = TERMINADO_CON_ERROR;
				}

			}
		} else if (string_starts_with(instruccion[3], "EC")) {
			if (!conozco_la_memoria(criterio_EC, una_memoria)) {
				list_add(criterio_EC, una_memoria);
				log_info(logger, "Se asocia la memoria id = %d al criterio Eventual Consistency", una_memoria->id);
			} else {
				log_warning(logger, "La memoria ya estaba bajo el criterio Evantual Consistency");
			}
		} else {
			if (!conozco_la_memoria(criterio_SHC, una_memoria)) {
				list_add(criterio_SHC, una_memoria);
				log_info(logger, "Se asocia la memoria id = %d al criterio Strong Hash Consistency", una_memoria->id);
				journal(criterio_SHC);
				if (list_size(criterio_SHC) == 1) {
					generar_diccionario();
					log_info(logger, "Diccionario generado");

				} else {
					log_info(logger, "Al agregarse una memoria al criterio se actualiza el diccionario");
					dictionary_put(diccionario_shc_memorias, string_itoa(dictionary_size(diccionario_shc_memorias)), una_memoria);
					log_info(logger, "Diccionario actualizado");
				}
			} else {
				log_warning(logger, "La memoria ya estaba bajo el criterio Strong Hash Consistency");
			}
		}
	} else {
		log_error(logger, "No se dispone de la memoria");
		script->estado_script = TERMINADO_CON_ERROR;
	}
	pthread_mutex_unlock(&mutex_lectura_memorias);
	for (int i = 0; instruccion[i] != NULL; i++)
		free(instruccion[i]);
	free(instruccion);

	sleep(retardo);
}

S_Memoria* buscar_memoria_por_id(S_Memoria* una_memoria) {
	for (int i = 0; 1; i++) {
		S_Memoria* aux = list_get(memorias, i);
		if (aux->id == una_memoria->id) {
			return aux;
		}
	}
}

void generar_diccionario() {
	int cantidad_shc_memorias = list_size(criterio_SHC);
	for (int i = 0; i < cantidad_shc_memorias; i++) {
		char* index = string_itoa(i);
		dictionary_put(diccionario_shc_memorias, index, list_get(criterio_SHC, i));
		free(index);
	}
}

void journal(t_list* lista) {
	void funcion_de_journal(S_Memoria* una_memoria) {
		Paquete* respuesta;
		int socket_servidor = conectar(una_memoria->ip_m, una_memoria->puerto_m, KERNEL);
		if (socket_servidor == -1) {
			log_warning(logger, "Memoria %d no disponible para Journal, se quita de la lista de memorias conocidas", una_memoria->id);
			if (conozco_la_memoria(criterio_SC, una_memoria)) {
				remover_de_lista(criterio_SC, una_memoria);
			}
			if (conozco_la_memoria(criterio_EC, una_memoria)) {
				remover_de_lista(criterio_EC, una_memoria);
			}
			if (conozco_la_memoria(criterio_SHC, una_memoria)) {
				remover_de_lista(criterio_SHC, una_memoria);
				//Como saco una memoria del criterio SHC actualizo el diccionario
				log_info(logger, "Se bajó una memoria asociada al SHC Consistency, por lo que se genera el diccionario nuevamente");
				dictionary_clean(diccionario_shc_memorias);
				generar_diccionario();
				log_info(logger, "Diccionario actualizado");
			}

			eliminar_de_lista_de_memorias(una_memoria);

			return;
		}
		//una_memoria->estado = OCUPADA;
		log_info(logger, "Se envía la instrucción JOURNAL a la memoria %d", una_memoria->id);
		log_info(logger, "Se obtiene respuesta del journal:");
		//	log_info(logger, "Envio la instruccion journal al socket %d", socket_servidor); //todo ojo
		enviar(socket_servidor, "JOURNAL");
		respuesta = recibir_paquete(socket_servidor);
		if (close(socket_servidor) == -1) {
			log_warning(logger, "Error al cerrar el socket de la función journal..");
		}
		log_info(logger, respuesta->buffer->stream);
		eliminar_paquete(respuesta);
		log_info(logger, "Se terminó la ejecución del la instrucción JOURNAL para la memoria %d", una_memoria->id);
		//una_memoria->estado = DISPONIBLE;
	}
	list_iterate(lista, (void*) funcion_de_journal);
}

void funcion_run(Script* script, int quantum) {

	log_info(logger, "Ejecuto run");

//Cargo la instrucción de la primera línea del archivo .lql
	pthread_mutex_lock(&mutex_carrera);
	script = cargar_instruccion_del_archivo(script);
	pthread_mutex_unlock(&mutex_carrera);

	while (quantum && ((script->estado_script) == PENDIENTE)) {

		log_info(logger, "Se pasa a ejecutar la instrucción %s %s del script id %d", script->cod_instruccion, script->datos, script->id_script);

		ejecutar_las_request(script, quantum);

		free(script->cod_instruccion);
		free(script->datos);

		script->lineas_pendientes -= 1;

		log_warning(logger, "Quedan %d lineas del script", script->lineas_pendientes);					//todo ojo

		if (script->estado_script != TERMINADO_CON_ERROR) {

			if (!script->lineas_pendientes) {
				script->estado_script = TERMINADO_SIN_ERROR;
			} else {
				script = cargar_instruccion_del_archivo(script);
			}
		}
		quantum--;
	}

	if (script->estado_script == TERMINADO_SIN_ERROR) {
		log_info(logger, "El script de id = %d ha terminado correctamente y se pasa a la cola de exit", script->id_script);
		pthread_mutex_lock(&mutex_carrera);
		queue_push(cola_exit, script);
		pthread_mutex_unlock(&mutex_carrera);

	} else if (script->estado_script == TERMINADO_CON_ERROR) {
		log_info(logger, "Se ha detectado un error en el script de id = %d, por lo que se deja de ejecutar y se pasa a la cola de exit", script->id_script);
		pthread_mutex_lock(&mutex_carrera);
		queue_push(cola_exit, script);
		pthread_mutex_unlock(&mutex_carrera);

	} else {
		log_info(logger, "Al script de id = %d se le ha terminado el quantum y vuelve a la cola de ready", script->id_script);
		/*  Seteo el código de instrucción para que cuando se vuelva a ejecutar vuelva a ingresar a la función RUN
		 *  al ppio de la función se pisará el código pora la instrucción correspondiente
		 */
		free(script->cod_instruccion);
		free(script->datos);
		script->cod_instruccion = "RUN";
		pthread_mutex_lock(&mutex_carrera);
		queue_push(cola_ready, script);
		pthread_mutex_unlock(&mutex_carrera);
		sem_post(&s_hay_elementos_en_ready);
	}
}

Script* cargar_instruccion_del_archivo(Script* un_script) {

	char * instruccion = instruccion_del_archivo_segun_linea((un_script->cantidad_de_lineas - un_script->lineas_pendientes), un_script->path);
	char* instruccion_cortada = string_new();
	char ** instruccion_separada;
	//	log_info(logger, "%s", instruccion);
	if (string_ends_with(instruccion, "\n")) {

		instruccion_cortada = string_substring(instruccion, 0, string_length(instruccion) - 1);

	} else {
		string_append(&instruccion_cortada, instruccion);
	}

	free(instruccion);

	instruccion_separada = string_n_split(instruccion_cortada, 2, " ");
	un_script->cod_instruccion = instruccion_separada[0];
	un_script->datos = instruccion_separada[1];

	free(instruccion_separada);

	if (analisis_instruccion(instruccion_cortada)) {
		un_script->estado_script = PENDIENTE;
	} else {
		un_script->estado_script = TERMINADO_CON_ERROR;
		log_error(logger, "El script falló al intentar cargar una instrucción del archivo .lql");
	}

	free(instruccion_cortada);

	return un_script;
}

void funcion_metrics(Script* script) {

	pthread_mutex_lock(&mutex_metricas);
	float cantidad_operaciones = SC->cantidad_inserts + SC->cantidad_selects + SHC->cantidad_inserts + SHC->cantidad_selects + EC->cantidad_inserts + EC->cantidad_selects;
	log_info(logger, "Muestro Metrics para criterio Strong Consistency");
	if (SC->cantidad_selects == 0) {
		log_info(logger, "Read Latency / 30s = 0");
	} else {
		log_info(logger, "Read Latency / 30s = %.2f", (SC->duracion_select / SC->cantidad_selects));
	}
	if (SC->cantidad_inserts == 0) {
		log_info(logger, "Write Latency / 30s = 0");
	} else {
		log_info(logger, "Write Latency / 30s = %.2f", (SC->duracion_insert / SC->cantidad_inserts));
	}

	log_info(logger, "Reads / 30s = %.0f", SC->cantidad_selects);
	log_info(logger, "Writes / 30s = %.0f", SC->cantidad_inserts);

	if (cantidad_operaciones == 0) {
		log_info(logger, "Memory Load = 0 %%");
	} else {
		log_info(logger, "Memory Load = %.2f %%", ((SC->cantidad_selects + SC->cantidad_inserts) / cantidad_operaciones) * 100);
	}

	log_info(logger, "Muestro Metrics para criterio Strong Hash Consistency");
	if (SHC->cantidad_selects == 0) {
		log_info(logger, "Read Latency / 30s = 0");
	} else {
		log_info(logger, "Read Latency / 30s = %.2f", (SHC->duracion_select / SHC->cantidad_selects));
	}
	if (SHC->cantidad_inserts == 0) {
		log_info(logger, "Write Latency / 30s = 0");
	} else {
		log_info(logger, "Write Latency / 30s = %.2f", (SHC->duracion_insert / SHC->cantidad_inserts));
	}

	log_info(logger, "Reads / 30s = %.0f", SHC->cantidad_selects);
	log_info(logger, "Writes / 30s = %.0f", SHC->cantidad_inserts);

	if (cantidad_operaciones == 0) {
		log_info(logger, "Memory Load = 0 %%");
	} else {
		log_info(logger, "Memory Load = %.2f %%", ((SHC->cantidad_selects + SHC->cantidad_inserts) / cantidad_operaciones) * 100);
	}

	log_info(logger, "Muestro Metrics para criterio Eventual Consistency");
	if (EC->cantidad_selects == 0) {
		log_info(logger, "Read Latency / 30s = 0");
	} else {
		log_info(logger, "Read Latency / 30s = %.2f", (EC->duracion_select / EC->cantidad_selects));
	}
	if (EC->cantidad_inserts == 0) {
		log_info(logger, "Write Latency / 30s = 0");
	} else {
		log_info(logger, "Write Latency / 30s = %.2f", (EC->duracion_insert / EC->cantidad_inserts));
	}

	log_info(logger, "Reads / 30s = %.0f", EC->cantidad_selects);
	log_info(logger, "Writes / 30s = %.0f", EC->cantidad_inserts);

	if (cantidad_operaciones == 0) {
		log_info(logger, "Memory Load = 0 %%");
	} else {
		log_info(logger, "Memory Load = %.2f %%", ((EC->cantidad_selects + EC->cantidad_inserts) / cantidad_operaciones) * 100);
	}

	limpiar_consistencias(SC);
	limpiar_consistencias(SHC);
	limpiar_consistencias(EC);

	script->estado_script = TERMINADO_SIN_ERROR;

	pthread_mutex_unlock(&mutex_metricas);
}
