#include "API-Kernel.h"

//-----------------------------------------------------------------Funciones principales--------------------------------------------------------------

void init_logger() {
	logger = log_create("log.log", "Logger", 1, LOG_LEVEL_INFO);
	log_info(logger, "Se ha creado un log.");
}

void loggear(char* leido) {
	log_info(logger, leido);
}

void init_config() {
	//char ruta_absoluta[PATH_MAX];
	//config = config_create(realpath("configuracion_kernel.config", ruta_absoluta));
	config = config_create(PATH_CONFIG);
	log_info(logger, "Se generó la conexión con archivo de configuración.");
	cargar_la_struct_con_la_config();

}

void cargar_la_struct_con_la_config() {
	configuracion_kernel = malloc(sizeof(struct Config));
	configuracion_kernel->ip_m = string_new();
	configuracion_kernel->puerto_m = string_new();

	string_append(&configuracion_kernel->ip_m, leer_configuracion_para_string("IP_MEMORIA"));
	string_append(&configuracion_kernel->puerto_m, leer_configuracion_para_string("PUERTO_MEMORIA"));

	configuracion_kernel->metadata_refresh = leer_configuracion_para_int("METADATA_REFRESH");
	configuracion_kernel->multiprocesamiento = leer_configuracion_para_int("MULTIPROCESAMIENTO");
	configuracion_kernel->quantum = leer_configuracion_para_int("QUANTUM");
	configuracion_kernel->sleep_ejecucion = leer_configuracion_para_int("SLEEP_EJECUCION");
	configuracion_kernel->sleep_memorias = leer_configuracion_para_int("SLEEP_MEMORIAS");
	configuracion_kernel->run_metrics = leer_configuracion_para_int("RUN_METRICS");
	log_info(logger, "Se cargaron todos los parámetros del archivo de configuración del kernel");

}

void cargar_parametros_modificados() {

	config = config_create(PATH_CONFIG);
	log_info(logger, "Se generó una nueva conexión con el archivo de configuración debido a la modificación.");
	configuracion_kernel->metadata_refresh = leer_configuracion_para_int("METADATA_REFRESH");
	configuracion_kernel->sleep_ejecucion = leer_configuracion_para_int("SLEEP_EJECUCION");
	configuracion_kernel->quantum = leer_configuracion_para_int("QUANTUM");
	configuracion_kernel->sleep_memorias = leer_configuracion_para_int("SLEEP_MEMORIAS");
	configuracion_kernel->run_metrics = leer_configuracion_para_int("RUN_METRICS");
	log_info(logger, "Se actualizaron los parámetros del archivo de configuración");

}

char* leer_configuracion_para_string(char* parametro) {
	char* valor;
	valor = config_get_string_value(config, parametro);
	return valor;
}

int leer_configuracion_para_int(char* parametro) {
	int valor;
	valor = config_get_int_value(config, parametro);
	return valor;
}

void opciones() {
	printf("Seleccione la opcion deseada:\n");
	printf("-----------------------------------------------\n");
	printf("COMANDO			- Descripcion\n");
	printf("-----------------------------------------------\n");
	printf("SELECT			- Obtener key\n");
	printf("INSERT			- Creación y/o actualización de key\n");
	printf("CREATE			- Creación de una nueva tabla\n");
	printf("DESCRIBE		- Obtener metadata\n");
	printf("DROP			- Eliminación de tabla\n");
	printf("JOURNAL			- Enviar manual de datos de memoria a FileSystem\n");
	printf("ADD MEMORY		- Asignar una memoria a un criterio\n");
	printf("RUN			- Ejecutar archivo LQL\n");
	printf("METRICS			- Obtener métricas\n");
	printf("SALIR			- Salir del sistema\n");
	printf("-----------------------------------------------\n");
	printf("-----------------------------------------------\n");
	printf("Ingrese Input\n");
}

void* leer_consola() {
	//Este semaforo es para empezar a mandar request luego de cargar las primeras memorias que informa el pool
	sem_wait(&s_espera_conexion_con_pool);

	int flag_instruccion_valida;
	log_info(logger, "Leyendo consola...");

	opciones();

	char* leido = readline(">");
	while (strncmp(leido, "", 1) != 0) {
		//loggear(leido);
		flag_instruccion_valida = analisis_instruccion(leido);
		if (flag_instruccion_valida) {
			cargar_instruccion_en_cola_new(leido);
		} else {
			log_warning(logger, "Entrada inválida!");
			log_info(logger, "Volver a ingresar Opción");
		}
		free(leido);
		leido = readline(">");

	}

	free(leido);

	return NULL;
}

int analisis_instruccion(char* instruccion_leida) {
	char ** instruccion_separada = string_split(instruccion_leida, " ");
	int largo = largo_instruccion(instruccion_separada);
	int respuesta = 0;

	if ((largo > 3) && string_equals_ignore_case(instruccion_separada[0], "INSERT")) {
		int cuarto_argumento = 1;
		int segundo_arg;
		segundo_arg = validar_numero(instruccion_separada[2]);

		for (int i = 0; instruccion_separada[i] != NULL; i++)
			free(instruccion_separada[i]);
		free(instruccion_separada);

		instruccion_separada = string_n_split(instruccion_leida, 4, " ");
		if (!string_ends_with(instruccion_separada[3], "\"")) {
			char ** aux = string_split(instruccion_separada[3], "\"");
			cuarto_argumento = validar_numero(aux[1]);
			for (int i = 0; aux[i] != NULL; i++)
				free(aux[i]);
			free(aux);
		}
		if (segundo_arg && cuarto_argumento) {
			respuesta = 1;
		}

	} else if ((largo == 3) && string_equals_ignore_case(instruccion_separada[0], "SELECT")) {
		respuesta = validar_numero(instruccion_separada[2]);
	} else if ((largo == 5) && string_equals_ignore_case(instruccion_separada[0], "CREATE")) {
		int tercer_arg, cuarto_arg, consistencia;
		consistencia = validar_consistencia(instruccion_separada[2]);
		tercer_arg = validar_numero(instruccion_separada[3]);
		cuarto_arg = validar_numero(instruccion_separada[4]);
		if (tercer_arg && cuarto_arg && consistencia) {
			respuesta = 1;
		}
	} else if ((largo < 3) && string_equals_ignore_case(instruccion_separada[0], "DESCRIBE")) {
		respuesta = 1;
	} else if ((largo == 2) && string_equals_ignore_case(instruccion_separada[0], "DROP")) {
		respuesta = 1;
	} else if ((largo == 1) && string_equals_ignore_case(instruccion_separada[0], "JOURNAL")) {
		respuesta = 1;
	} else if ((largo == 5) && string_equals_ignore_case(instruccion_separada[0], "ADD")) {
		int segundo_arg, consistencia, to, memory;
		segundo_arg = validar_numero(instruccion_separada[2]);
		consistencia = validar_consistencia(instruccion_separada[4]);
		memory = string_equals_ignore_case("MEMORY", instruccion_separada[1]);
		to = string_equals_ignore_case("TO", instruccion_separada[3]);
		if (segundo_arg && consistencia && to && memory) {
			respuesta = 1;
		}
	} else if ((largo == 2) && string_equals_ignore_case(instruccion_separada[0], "RUN")) {
		respuesta = 1;
	} else if ((largo == 1) && string_equals_ignore_case(instruccion_separada[0], "METRICS")) {
		respuesta = 1;
	} else if ((largo == 1) && string_equals_ignore_case(instruccion_leida, "SALIR")) {
		for (int i = 0; instruccion_separada[i] != NULL; i++)
			free(instruccion_separada[i]);
		free(instruccion_separada);
		free(instruccion_leida);
		terminar_programa();
	}

	for (int i = 0; instruccion_separada[i] != NULL; i++)
		free(instruccion_separada[i]);
	free(instruccion_separada);
	return respuesta;
}

int largo_instruccion(char** instruccion) {
	int i;
	for (i = 0; instruccion[i] != NULL; i++)
		;
	return i;
}

int validar_numero(char* texto) {
	int hay_un_caracter = 0;
	for (int i = 0; texto[i] != '\0'; i++) {
		//Chequeo si algun caracter es una letra
		if (isalpha(texto[i]) != 0)
			hay_un_caracter++;
	}

	if (hay_un_caracter) {
		return 0;
	} else {
		return 1;
	}

}

int validar_consistencia(char* texto) {
	int respuesta;
	if (string_equals_ignore_case(texto, "SC")) {
		respuesta = 1;
	} else if (string_equals_ignore_case(texto, "SHC")) {
		respuesta = 1;
	} else if (string_equals_ignore_case(texto, "EC")) {
		respuesta = 1;
	} else {
		respuesta = 0;
	}

	return respuesta;
}

void cargar_instruccion_en_cola_new(char* instruccion) {

	char ** instruccion_separada = string_n_split(instruccion, 2, " ");
	Instruccion* instruccion_s = malloc(sizeof(Instruccion));
	instruccion_s->cod_instruccion = string_new();
	instruccion_s->datos = string_new();
	string_append(&instruccion_s->cod_instruccion, instruccion_separada[0]);

	if (instruccion_separada[1] != NULL) {
		string_append(&instruccion_s->datos, instruccion_separada[1]);
	}

	log_info(logger, "La instrucción %s ingresó a la cola de New ", instruccion_s->cod_instruccion);
	pthread_mutex_lock(&mutex_carrera);
	queue_push(cola_new, instruccion_s);
	pthread_mutex_unlock(&mutex_carrera);
	sem_post(&s_hay_elementos_en_new);
	for (int i = 0; instruccion_separada[i] != NULL; i++)
		free(instruccion_separada[i]);
	free(instruccion_separada);

}

void init_listas_de_mem_y_criterios() {
	memorias = list_create();
	memorias_aux = list_create();
	//SC deberías ser un sólo elemento, si lo trabajo como lista es para que sean polimórficas entre si, aún no se si tiene alguna ventaja
	criterio_SC = list_create();
	criterio_SHC = list_create();
	criterio_EC = list_create();
	metadata = list_create();
	diccionario_shc_memorias = dictionary_create();
}

void* consultar_memorias() {
	int retardo;
	int i = 0;
	int socket_pool;
	char* ip_principal = configuracion_kernel->ip_m;
	char* puerto_principal = configuracion_kernel->puerto_m;

	while (flag_ejecutar) {

		pthread_mutex_lock(&mutex_memorias);
		socket_pool = conectar(ip_principal, puerto_principal, KERNEL);

		if (socket_pool == -1) {
			log_warning(logger, "No se puede conectar a la memoria principal del archivo de configuración");
			S_Memoria* aux = malloc(sizeof(S_Memoria));
			aux->puerto_m = puerto_principal;
			remover_de_lista(memorias, aux);
			free(aux);

			if (list_size(memorias) == 0) {
				if (flag_primera_vez) {
					log_error(logger, "No conozco otra memoria, se cierra el kernel");
					exit(1);
				}
				log_error(logger, "No conozco memorias para operar, por lo que tengo que terminar la ejecución");
				terminar_programa();
			} else {
				S_Memoria* auxiliar;
				do {
					auxiliar = list_get(memorias, i);
					i++;
					if (auxiliar != NULL) {
						socket_pool = conectar(auxiliar->ip_m, auxiliar->puerto_m, KERNEL);
					}

				} while (auxiliar != NULL && (socket_pool == -1));
				if (auxiliar == NULL) {
					log_error(logger, "No me puedo conectar a ninguna de las memorias que conozco");
					if (flag_primera_vez) {
						exit(1);
					}
					terminar_programa();
				}
			}
		}
		i = 0;

		pthread_mutex_lock(&mutex_ingreso_archivo_config);
		retardo = configuracion_kernel->sleep_memorias / 1000;
		pthread_mutex_unlock(&mutex_ingreso_archivo_config);
		//log_warning(logger, "Envio la instruccion MEMORIA al socket %d", socket_pool);
		enviar(socket_pool, "MEMORIA");
		Paquete* paquete_memoria = recibir(socket_pool);
		//log_warning(logger, "Obtuve respuesta de MEMORIA: %s " , paquete_memoria->buffer->stream);
		if (close(socket_pool) == -1) {
			log_warning(logger, "Error al cerrar el socket la función que consulta memorias...");
		}
		char** auxiliar_memorias = string_split(paquete_memoria->buffer->stream, ";");

		for (int i = 0; auxiliar_memorias[i] != NULL; i++) {

			char** auxiliar_una_memoria = string_split(auxiliar_memorias[i], "-");
			S_Memoria* memoria_pool = malloc(sizeof(S_Memoria));

			memoria_pool->ip_m = string_new();
			memoria_pool->puerto_m = string_new();

			string_append(&memoria_pool->ip_m, auxiliar_una_memoria[0]);
			string_append(&memoria_pool->puerto_m, auxiliar_una_memoria[1]);
			memoria_pool->id = atoi(auxiliar_una_memoria[2]);

			free(auxiliar_una_memoria[0]);
			free(auxiliar_una_memoria[1]);
			free(auxiliar_una_memoria[2]);
			free(auxiliar_una_memoria);

			free(auxiliar_memorias[i]);
			list_add(memorias_aux, memoria_pool);

			//Sólo logueo cuando hay memorias que no conocia
			if (!conozco_la_memoria(memorias, memoria_pool)) {
				log_info(logger, "Se obtuvo memoria del pool, nro id: %d", memoria_pool->id);
				//memoria_pool->estado = DISPONIBLE;
			}

		}
		free(auxiliar_memorias);

		//Este for es para verificar si se cayó alguna memoria y loguea esto por pantalla. FALTA TESTEAR con memorias reales
		pthread_mutex_lock(&mutex_lectura_memorias);
		for (int i = 0; i < list_size(memorias); i++) {
			S_Memoria* memoria_a_chequear = list_get(memorias, i);

			if (!conozco_la_memoria(memorias_aux, memoria_a_chequear)) {
				log_info(logger, "Se cayo la memoria con puerto = %d del pool", memoria_a_chequear->id);

				//Esta funcion es para remover de la lista de criterios la memoria caida
				if (conozco_la_memoria(criterio_SC, memoria_a_chequear)) {
					remover_de_lista(criterio_SC, memoria_a_chequear);
				}
				if (conozco_la_memoria(criterio_EC, memoria_a_chequear)) {
					remover_de_lista(criterio_EC, memoria_a_chequear);
				}
				if (conozco_la_memoria(criterio_SHC, memoria_a_chequear)) {
					remover_de_lista(criterio_SHC, memoria_a_chequear);
					//Como saco una memoria del criterio SHC actualizo el diccionario
					log_info(logger, "Se bajó una memoria asociada al SHC Consistency, por lo que se genera el diccionario nuevamente");
					dictionary_clean(diccionario_shc_memorias);
					generar_diccionario();
					log_info(logger, "Diccionario actualizado");
				}

				eliminar_de_lista_de_memorias(memoria_a_chequear);

			}
			//free(memoria_a_chequear);
		}
		list_clean(memorias);
		list_add_all(memorias, memorias_aux);
		pthread_mutex_unlock(&mutex_lectura_memorias);
		list_clean(memorias_aux);
		eliminar_paquete(paquete_memoria);
		sem_post(&s_espera_conexion_con_pool);
		pthread_mutex_unlock(&mutex_memorias);
		sleep(retardo);
	}

	return NULL;
}

void remover_de_lista(t_list* lista, S_Memoria* una_memoria) {
	S_Memoria* memo_auxiliar;
	for (int i = 0; i < list_size(lista); i++) {
		memo_auxiliar = list_get(lista, i);
		if (!strcmp(memo_auxiliar->puerto_m, una_memoria->puerto_m)) {
			list_remove(lista, i);
		}
	}

}

void eliminar_de_lista_de_memorias(S_Memoria* una_memoria) {
	S_Memoria* memo_auxiliar;
	for (int i = 0; i < list_size(memorias); i++) {
		memo_auxiliar = list_get(memorias, i);
		if (!strcmp(memo_auxiliar->puerto_m, una_memoria->puerto_m)) {
			list_remove(memorias, i);
		}
	}
	if (memo_auxiliar == NULL) {
		return;
	}
	free(memo_auxiliar->ip_m);
	free(memo_auxiliar->puerto_m);
	free(memo_auxiliar);
}

int conozco_la_memoria(t_list* lista_memorias, S_Memoria* una_memoria) {
	int respuesta = 0;
	for (int i = 0; i < list_size(lista_memorias); i++) {
		S_Memoria* memo_auxiliar = list_get(lista_memorias, i);
		if (una_memoria->id == memo_auxiliar->id) {
			respuesta = 1;
			break;
		}
	}

	return respuesta;
}

//void* obtener_la_memoria(t_list* lista_memorias, S_Memoria* una_memoria) {
//	int buscar_id(S_Memoria* mem) {
//		return (mem->id == una_memoria->id);
//	}
//	return list_find(lista_memorias, (void*) buscar_id);
//}

void terminar_programa() {
	log_info(logger, "Se cierra el Kernel");
	flag_ejecutar = 0;
	pthread_mutex_destroy(&mutex_id_script);
	pthread_mutex_destroy(&mutex_exec);
	pthread_mutex_destroy(&mutex_ingreso_archivo_config);
	pthread_mutex_destroy(&mutex_lectura_memorias);
	pthread_mutex_destroy(&mutex_metricas);
	pthread_mutex_destroy(&mutex_metadata);
	pthread_mutex_destroy(&mutex_memorias);
	pthread_mutex_destroy(&mutex_carrera);

	sem_destroy(&s_hay_elementos_en_new);
	sem_destroy(&s_hay_elementos_en_ready);
	sem_destroy(&s_espera_conexion_con_pool);
	sem_destroy(&s_se_termino_un_exec);
	sem_destroy(&s_espero_el_primer_describe);

	log_destroy(logger);

	config_destroy(config);

	list_destroy_and_destroy_elements(memorias, (void*) destructor_elementos_lista);
	list_destroy_and_destroy_elements(memorias_aux, (void*) destructor_elementos_lista);
	list_destroy(criterio_EC);
	list_destroy(criterio_SHC);
	list_destroy(criterio_SC);
	list_destroy_and_destroy_elements(metadata, (void*) destructor_elementos_lista_meta);

	dictionary_destroy(diccionario_shc_memorias);

	free(SHC);
	free(EC);
	free(SC);
	destruir_queue();

//	pthread_exit(&hilo_consulta_memorias);
//	pthread_exit(&hilo_notif);
//	pthread_exit(&hilo_metricas);
//	pthread_exit(&hilo_consola);
//	pthread_exit(&hilo_cola_ejex);
//	pthread_exit(&hilo_cola_ready);
//	pthread_exit(&hilo_describe_Scheduleado);

	free(configuracion_kernel->ip_m);
	free(configuracion_kernel->puerto_m);
	free(configuracion_kernel);
	exit(1);

}

void destruir_queue() {
//	log_info(logger, "Tamanio NEW: %d", cola_new->elements->elements_count);
//	log_info(logger, "Tamanio READY: %d", cola_ready->elements->elements_count);
//	log_info(logger, "Tamanio EXECUTE: %d", cola_execute->elements->elements_count);
//	log_info(logger, "Tamanio EXIT: %d", cola_exit->elements->elements_count);

	queue_destroy(cola_new);
	queue_destroy(cola_ready);
	queue_destroy(cola_execute);
	queue_destroy(cola_exit);

//	if (queue_size(cola_new)) {
//		queue_destroy_and_destroy_elements(cola_new, (void*) destructor_instr);
//	} else {
//		queue_destroy(cola_new);
//	}
//	if (queue_size(cola_ready)) {
//		queue_destroy_and_destroy_elements(cola_ready, (void*) destructor_script);
//	} else {
//		queue_destroy(cola_ready);
//	}
//	if (queue_size(cola_execute)) {
//		queue_destroy_and_destroy_elements(cola_execute, (void*) destructor_script);
//	} else {
//		queue_destroy(cola_execute);
//	}
//	if (queue_size(cola_exit)) {
//		queue_destroy_and_destroy_elements(cola_exit, (void*) destructor_script);
//	} else {
//		queue_destroy(cola_exit);
//	}

}

void destructor_instr(Instruccion* instruccion) {
	free(instruccion->datos);
	free(instruccion->cod_instruccion);
	free(instruccion);
}

void destructor_script(Script* script) {
	free(script->cod_instruccion);
	free(script->datos);
	//free(script->path);
	free(script);
}

void destructor_elementos_lista(S_Memoria* memoria) {
	free(memoria->ip_m);
	free(memoria->puerto_m);
	free(memoria);
}

void destructor_elementos_lista_meta(S_Metadata* meta) {

	free(meta->nombre_tabla);
	free(meta->particiones);
	free(meta->tipo_consistencia);
	free(meta->tiempo_compactacion);
	free(meta);
}

void limpiar_consistencias(S_Consistency* una_consistencia) {
	una_consistencia->cantidad_selects = 0;
	una_consistencia->cantidad_inserts = 0;
	//una_consistencia->cantidad_operaciones = 0;
	una_consistencia->t0_insert = 0;
	una_consistencia->tf_insert = 0;
	una_consistencia->duracion_insert = 0;
	una_consistencia->t0_select = 0;
	una_consistencia->tf_select = 0;
	una_consistencia->duracion_select = 0;
	una_consistencia->memory_load = 0;

}

