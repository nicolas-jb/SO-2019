#define IP "127.0.0.2"
#define PUERTO "8081"

//#include "LFS.h"
#include "sockets.h"

/*
 * Crea el semaforo para la tabla indicada y lo agrega a la lista de semaforos
 */
void iniciar_semaforo(char* nombre_tabla) {
	Semaforo_de_una_tabla* semaforo = malloc(sizeof(Semaforo_de_una_tabla));
	semaforo->nombre_tabla = string_new();
	string_append(&semaforo->nombre_tabla, nombre_tabla);
	pthread_mutex_init(&semaforo->semaforo_c, NULL);
	pthread_mutex_init(&semaforo->semaforo_particion, NULL);

	list_add(lista_semaforos, semaforo);
}

/*
 * Busca el semaforo de la tabla solicitada. (De no estar en la lista de semaforos, retorna NULL)
 */
Semaforo_de_una_tabla* buscar_semaforo(char* nombre_tabla) {
	bool mismo_nombre(Semaforo_de_una_tabla* semaforo) {
//		log_info(logger, "Tabla que esta en la lista: %s", semaforo->nombre_tabla);
//		log_info(logger, "Tabla que me pasan por parametro: %s", nombre_tabla);
		return string_equals_ignore_case(semaforo->nombre_tabla, nombre_tabla);
	}

//	pthread_mutex_lock(&mutex_lista_semaforos); //fixme semaforo de semaforos
	Semaforo_de_una_tabla* semaforo_encontrado = list_find(lista_semaforos, (void*) mismo_nombre);
//	pthread_mutex_unlock(&mutex_lista_semaforos); //fixme semaforo de semaforos

	return semaforo_encontrado;
}

/*
 * Lockea el semaforo de la tabla solicitada
 */
void lock_semaforo_compactar(char* nombre_tabla) {
	Semaforo_de_una_tabla* semaforo = buscar_semaforo(nombre_tabla);
	pthread_mutex_lock(&semaforo->semaforo_c);
}

/*
 * Lockea el semaforo de la tabla solicitada
 */
void lock_semaforo_particion(char* nombre_tabla) {
	Semaforo_de_una_tabla* semaforo = buscar_semaforo(nombre_tabla);
	pthread_mutex_lock(&semaforo->semaforo_particion);
}

/*
 * Desblockea el semaforo de la tabla solicitada
 */
void unlock_semaforo_compactar(char* nombre_tabla) {
	Semaforo_de_una_tabla* semaforo = buscar_semaforo(nombre_tabla);
	pthread_mutex_unlock(&semaforo->semaforo_c);
}

/*
 * Desblockea el semaforo de la tabla solicitada
 */
void unlock_semaforo_particion(char* nombre_tabla) {
	Semaforo_de_una_tabla* semaforo = buscar_semaforo(nombre_tabla);
	pthread_mutex_unlock(&semaforo->semaforo_particion);
}

/*
 * Elimina un semaforo liberando su memoria
 */
void eliminar_semaforo(Semaforo_de_una_tabla* semaforo) {
	free(semaforo->nombre_tabla);
	pthread_mutex_destroy(&semaforo->semaforo_c);
	pthread_mutex_destroy(&semaforo->semaforo_particion);
	free(semaforo);
}

/*
 * Busca un semaforo por el nombre de la tabla. Si lo encuentra lo elimina y retorna 1.
 * En caso de no encontrarlo retorna -1.
 */
int buscar_semaforo_por_nombre_tabla_y_eliminar_si_esta(char* nombre_tabla) {
	Semaforo_de_una_tabla* semaforo = buscar_semaforo(nombre_tabla);

	if (semaforo == NULL)
		return -1;

	bool mismo_nombre(Semaforo_de_una_tabla* semaforo) {
		return string_equals_ignore_case(semaforo->nombre_tabla, nombre_tabla);
	}

	list_remove_and_destroy_by_condition(lista_semaforos, (void*) mismo_nombre, (void*) eliminar_semaforo);

	return 1;
}

/*
 * Elimina la lista de semaforos y elimina cada registro liberando memoria.
 */
void eliminar_lista_de_semaforos() {
	list_destroy_and_destroy_elements(lista_semaforos, (void*) eliminar_semaforo);
}

/*
 * Crea la lista de semaforos
 */
void iniciar_lista_de_semaforos() {
	lista_semaforos = list_create();
}

int main(void) {
//  ---------------------INICIALIZACION---------------------
	lfs_encendido = 1; //fixme flag apagar sistema
	iniciar_logger();
	leer_config();
	iniciar_memtable();
	limpiar_memtable();
	inicializar_nombre_punto_montaje();
	inicializar_directorios();
	leer_metadata_LFS();
	iniciar_punto_montaje();

	pthread_mutex_init(&mutex_socket , NULL);
	pthread_attr_init(&attr); //fixme atribute detachable
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	iniciar_bitmap(cantidad_bloques_LFS);
	iniciar_lista_de_semaforos();

	pthread_mutex_init(&mutex_ingreso_archivo_config, NULL);
	pthread_mutex_init(&mutex_memtable, NULL);
	pthread_mutex_init(&mutex_bitarray, NULL);
	iniciar_compactaciones_de_tablas_existentes();

//

//	mostrar_bitarray();

//------------------------TESTS----------------------

//	char* nombre_tabla = "TABLITA2";
//	TIPO_CONSISTENCIA consistencia = SC;
//	int numero_particiones = 5;
//	int tiempo_compactacion = 15;
//
//	create(nombre_tabla, consistencia, numero_particiones, tiempo_compactacion);

//	nombre_tabla = "tabulani";
//	consistencia = EC;
//	numero_particiones = 3;
//	tiempo_compactacion = 10;
//	create(nombre_tabla, consistencia, numero_particiones, tiempo_compactacion);

//---------------------FIN TESTS---------------------

//  ---------------------INICIO HILOS---------------------
	pthread_create(&hilo_consola, NULL, leer_consola, NULL);
	pthread_create(&hilo_notificador, NULL, notificaciones, NULL);
	pthread_create(&hilo_lfs, NULL, manejar_requests, NULL);
	pthread_create(&hilo_dump, NULL, dump_lfs, NULL);
//
////  ---------------------FIN HILOS---------------------
//
	pthread_join(hilo_dump, NULL);
	//fixme Aca estaba el join de la consola, lo movi abajo
	pthread_join(hilo_lfs, NULL);
	pthread_join(hilo_notificador, NULL);
	pthread_join(hilo_consola, NULL);

//  ---------------------FINALIZACION------------------
//	close(socket_oyente);

	finalizar();
	return 0;
}

//------------------------------CONSOLA---------------------------------

void* leer_consola() {

	int flag_instruccion_valida;
	log_info(logger, "Leyendo consola...");

	opciones();

	char* leido = readline(">");

	while (strncmp(leido, "", 1) != 0) {
		log_info(logger, leido);
		flag_instruccion_valida = analisis_instruccion(leido);

		if (flag_instruccion_valida) {
			log_info(logger, "Instruccion valida");
			int mi_mismo = conectar(configuracion_lfs->ip, configuracion_lfs->puerto, LFS);
			enviar_mensaje(mi_mismo, leido);
			Paquete* respuesta = recibir_paquete(mi_mismo);
			log_info(logger, "%s", respuesta->buffer->stream);
			eliminar_paquete(respuesta);
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
		for (int i = 0; instruccion_separada[i] != NULL; i++) {
			free(instruccion_separada[i]);
		}
		free(instruccion_separada);
		instruccion_separada = string_n_split(instruccion_leida, 4, " ");
		if (!string_ends_with(instruccion_separada[3], "\"")) {
			char ** aux = string_split(instruccion_separada[3], "\"");
			cuarto_argumento = validar_numero(aux[1]);
			for (int j = 0; j < 3; j++)
				free(aux[j]);
			free(aux);
		}
		if (segundo_arg && cuarto_argumento) {
			respuesta = 1;
		}
		respuesta = validar_numero(instruccion_separada[2]);

	} else if ((largo == 5) && string_equals_ignore_case(instruccion_separada[0], "CREATE")) {
		int tercer_arg, cuarto_arg, consistencia;
		consistencia = validar_consistencia_consola(instruccion_separada[2]);
		tercer_arg = validar_numero(instruccion_separada[3]);
		cuarto_arg = validar_numero(instruccion_separada[4]);
		if (tercer_arg && cuarto_arg && consistencia) {
			respuesta = 1;
		}
	} else if ((largo == 3) && string_equals_ignore_case(instruccion_separada[0], "SELECT")) {
		respuesta = 1;
	} else if ((largo < 3) && string_equals_ignore_case(instruccion_separada[0], "DESCRIBE")) {
		respuesta = 1;
	} else if ((largo == 2) && string_equals_ignore_case(instruccion_separada[0], "DROP")) {
		respuesta = 1;
	} else if ((largo == 1) && string_equals_ignore_case(instruccion_leida, "SALIR")) {
		for (int i = 0; instruccion_separada[i] != NULL; i++)
			free(instruccion_separada[i]);
		free(instruccion_separada);
		finalizar();
	}

	for (int i = 0; instruccion_separada[i] != NULL; i++) {
		free(instruccion_separada[i]);
	}
	free(instruccion_separada);
	return respuesta;
}
//---------------------------------FIN CONSOLA----------------------------

void crear_hilo_por_cada_request(int socket, Modulo quien_soy) {
	struct sockaddr_in direccion_cliente;
	unsigned int tamanio_direccion = sizeof(struct sockaddr_in);
	int socket_cliente;


	int* nuevo_socket;
	while (lfs_encendido) {

		socket_cliente = accept(socket, (void*) &direccion_cliente, &tamanio_direccion);

		pthread_t hilo;
		nuevo_socket = malloc(sizeof(int*));
		*nuevo_socket = socket_cliente;
		log_info(logger, "Recibi un cliente.");

		if (recibir_saludo(socket_cliente, quien_soy) == -1) {
			log_info(logger, "Cliente desconocido");
			continue;
		}
		enviar_saludo(socket_cliente, quien_soy);

		//Crear un hilo.
		int que_paso = pthread_create(&hilo, &attr, atender_request, (void*) nuevo_socket);

//		log_info(logger, "Resultado de crear hilo: %d", que_paso);
		while (que_paso != 0) {
//			log_error(logger, "No se pudo clear el hilo, voy a intentar de vuelta");
//			pthread_join(hilo, NULL);
			que_paso = pthread_create(&hilo, &attr, atender_request, (void*) nuevo_socket);
		}
	}
}
//-----------------------------NOTIFICACIONES---------------------
void* notificaciones() {

	char buffer[BUF_LEN];
	int offset = 0;
	int file_descriptor = inotify_init();
	if (file_descriptor < 0) {
		log_error(logger, "error en inotify_init");
	}

	int watch_descriptor = inotify_add_watch(file_descriptor, "/home/utnso/Escritorio/tp-2019-1c-dale_que_SO_vo/LFS", IN_MODIFY);

	if (watch_descriptor == -1) {
		log_error(logger, "error en agregar monitor");
	}

	while (lfs_encendido) {
		offset = 0;
		int length = read(file_descriptor, buffer, BUF_LEN);
		if (length < 0) {
			log_error(logger, " error en read");
		}

		while (offset < length) {

			struct inotify_event *event = (struct inotify_event *) &buffer[offset];

			if (event->len) {
				if (event->mask & IN_MODIFY) {
					if (event->mask & IN_ISDIR) {
						log_warning(logger, "The directory %s was modified.\n", event->name);
					} else {
						log_warning(logger, "El archivo %s fue modificado.\n", event->name);
//						config_destroy(config);
						cargar_parametros_modificados();
					}
				}
			}
			offset += sizeof(struct inotify_event) + event->len;
		}
	}

	inotify_rm_watch(file_descriptor, watch_descriptor);
	close(file_descriptor);

	return NULL;

}

void cargar_parametros_modificados() {
	pthread_mutex_lock(&mutex_ingreso_archivo_config);
	config = config_create(PATH_CONFIG);
	log_info(logger, "Se generó una nueva conexión con el archivo de configuración debido a la modificación.");
	configuracion_lfs->retardo = config_get_int_value(config, "RETARDO") / 1000;
	configuracion_lfs->tiempo_dump = config_get_int_value(config, "TIEMPO_DUMP") / 1000;
	log_info(logger, "Se actualizaron los parámetros del archivo de configuración.");

	config_destroy(config);
	pthread_mutex_unlock(&mutex_ingreso_archivo_config);
}
//----------------------------------------------------------

void iniciar_punto_montaje() {

// 	cant_bloques = 8;             // Esta en el main asignado
// 	char* nombre_punto_montaje = "/home/utnso/Escritorio/tp-2019-1c-dale_que_SO_vo/LFS/tables";// LO PUSIMOS COMO GLOBAL
	char* carpeta_metadata = string_new();
	char* metadata_bin = string_new();
	char* bitarray_bin = string_new();

	string_append(&carpeta_metadata, nombre_punto_montaje);
	string_append(&carpeta_metadata, "/Metadata");

	string_append(&metadata_bin, carpeta_metadata);
	string_append(&metadata_bin, "/Metadata.bin");

	string_append(&bitarray_bin, carpeta_metadata);
	string_append(&bitarray_bin, "/Bitmap.bin");

	//Iniciando directorio de tablas
	crear_fichero(directorio_tablas);
	log_info(logger, "%s", directorio_tablas);

	//Iniciando directorio de metadata
	crear_fichero(carpeta_metadata);
	log_info(logger, "%s", carpeta_metadata);

	//Iniciando directorio de bloques
	crear_fichero(directorio_bloques);
	log_info(logger, "%s", directorio_bloques);

	//Inicializando bloques
	char* directorio_bloque_a_crear;
	int i;
	for (i = 0; i < cantidad_bloques_LFS; i++) {
		directorio_bloque_a_crear = seleccionar_bloque_a_manipular(i);
		if (existe_archivo(directorio_bloque_a_crear)) {
//			log_info(logger, "El bloque %d ya existe en LFS", i);
		} else {
			FILE* archivo_bloque = txt_open_for_append(directorio_bloque_a_crear);
			txt_close_file(archivo_bloque);
		}
		free(directorio_bloque_a_crear);
	}

	//-- fin inicializando bloques

	//Creo las tablas ya existentes antes de iniciar el programa en la memtable
	t_list* nombre_de_tablas_en_lfs = crear_listado_nombres_tablas();
	int cantidad_tablas_en_lfs = list_size(nombre_de_tablas_en_lfs);
	//ITERANDO POR TABLAS
	for (i = 0; i < cantidad_tablas_en_lfs; i++) {
		char* nombre_tabla_crear_memtable = list_get(nombre_de_tablas_en_lfs, i);
		Tabla* tabla = buscar_tabla(nombre_tabla_crear_memtable);
		tabla = crear_tabla(nombre_tabla_crear_memtable);
		list_add(memtable, tabla);
		free(nombre_tabla_crear_memtable);
	}
	list_destroy(nombre_de_tablas_en_lfs);

	//Libero memoria
	free(carpeta_metadata);
	free(metadata_bin);
	free(bitarray_bin);
}

Paquete* create(Paquete* paquete) {

	Paquete* resultado;
	char** auxiliar = string_n_split(paquete->buffer->stream, 4, " ");

	char* nombre_tabla = string_new();
	string_append(&nombre_tabla, auxiliar[0]);

	char* auxiliar_consistencia = string_new();
	string_append(&auxiliar_consistencia, auxiliar[1]);

	TIPO_CONSISTENCIA consistencia = convertir_consistencia(auxiliar_consistencia);
	int numero_particiones = atoi(auxiliar[2]);
	int tiempo_compactacion = atoi(auxiliar[3]) / 1000;

	free(auxiliar_consistencia);
	free(auxiliar[0]);
	free(auxiliar[1]);
	free(auxiliar[2]);
	free(auxiliar[3]);
	free(auxiliar);

	log_warning(logger, "INTENTO BLOQUEAR BITARRAY EN CREATE");
	pthread_mutex_lock(&mutex_bitarray);
	log_warning(logger, "BLOQUEO BITARRAY EN CREATE");

	// Validamos que hay suficiemtes bloques disponibles para asignar a las particiones de la nueva tabla
	if (cantidad_de_bloques_vacios() < numero_particiones) {
//		log_warning(logger, "No hay suficientes bloques vacios para asignar a las particiones de la tabla, ABORTANDO CREATE..");
		resultado = crear_paquete(ERROR, "No hay suficientes bloques vacios para asignar a las particiones de la tabla, ABORTANDO CREATE..");
		free(nombre_tabla);
		pthread_mutex_unlock(&mutex_bitarray);
		return resultado;
	}
	pthread_mutex_lock(&mutex_lista_semaforos); //fixme semaforo de semaforos
	iniciar_semaforo(nombre_tabla);
	pthread_mutex_unlock(&mutex_lista_semaforos); //fixme semaforo de semaforos

	//Armando path
	char* carpeta_tabla = string_new();
	string_append(&carpeta_tabla, directorio_tablas);
	string_append(&carpeta_tabla, "/");
	string_append(&carpeta_tabla, nombre_tabla);

	if (existe_carpeta(carpeta_tabla)) {
//		log_warning(logger, "La tabla %s ya existe en LFS", nombre_tabla);
		free(carpeta_tabla);
		free(nombre_tabla);
		resultado = crear_paquete(ERROR, "Ya existe la tabla, ABORTANDO CREATE..");
		pthread_mutex_unlock(&mutex_bitarray);
		return resultado;
	}
	char* consistency = validar_consistencia(consistencia);
	if (strcmp(consistency, "ERROR") == 0) {
		resultado = crear_paquete(ERROR, "Error al validar la consistencia de la tabla. ABORTANDO CREATE...");
		free(carpeta_tabla);
		free(nombre_tabla);
		pthread_mutex_unlock(&mutex_bitarray);
		return resultado;
	}

	//crear carpeta de tabla

	crear_fichero(carpeta_tabla);

	//Creando archivo metadata
	char* metadata_bin = string_new();
	string_append(&metadata_bin, carpeta_tabla);
	string_append(&metadata_bin, "/Metadata.bin");
	FILE* archivo_metadata = txt_open_for_append(metadata_bin);

	//Creamos registros de archivo metadata
	char* registro_consistencia = string_new();
	char* registro_numero_particiones = string_new();
	char* registro_compaction_time = string_new();

	string_append(&registro_consistencia, "CONSISTENCY=");
	string_append(&registro_consistencia, consistency);

	string_append(&registro_numero_particiones, "PARTITIONS=");
	char* partitions = string_itoa(numero_particiones);
	string_append(&registro_numero_particiones, partitions);
	free(partitions);

	string_append(&registro_compaction_time, "COMPACTION_TIME=");
	char* compaction_time = string_itoa(tiempo_compactacion);
	string_append(&registro_compaction_time, compaction_time);
	free(compaction_time);

	//Escribiendo archivo metadata
	txt_write_in_file(archivo_metadata, registro_consistencia);
	txt_write_in_file(archivo_metadata, "\n");

	txt_write_in_file(archivo_metadata, registro_numero_particiones);
	txt_write_in_file(archivo_metadata, "\n");

	txt_write_in_file(archivo_metadata, registro_compaction_time);
	txt_write_in_file(archivo_metadata, "\n");

	//Cierro archivo
	txt_close_file(archivo_metadata);

	//Crear particiones y asignar un bloque a cada una
	char* directorio_particion_a_crear;
	int i;
	for (i = 0; i < numero_particiones; i++) {
		directorio_particion_a_crear = seleccionar_particion_a_manipular(i, carpeta_tabla);
		FILE* archivo_particion = txt_open_for_append(directorio_particion_a_crear);
		txt_write_in_file(archivo_particion, "SIZE=0\nBLOQUES=[");
		int numero_de_primer_bloque_no_ocupado = devolver_numero_de_primer_bloque_no_ocupado();
		char* char_numero_de_primer_bloque_no_ocupado = string_itoa(numero_de_primer_bloque_no_ocupado);
		txt_write_in_file(archivo_particion, char_numero_de_primer_bloque_no_ocupado);
		txt_write_in_file(archivo_particion, "]");
		txt_close_file(archivo_particion);

		bitarray_set_bit(bitarray, numero_de_primer_bloque_no_ocupado);
		//Sincronizo bitarray con el archivo
		msync(cadena_bitarray, 2, MS_SYNC);

		free(directorio_particion_a_crear);
		free(char_numero_de_primer_bloque_no_ocupado);
	}
	log_warning(logger, "INTENTO DESBLOQUEAR BITARRAY EN CREATE");
	pthread_mutex_unlock(&mutex_bitarray);
	log_warning(logger, "DESBLOQUEO BITARRAY EN CREATE");

	//Libero memoria
	free(carpeta_tabla);
	free(metadata_bin);
	free(registro_consistencia);
	free(registro_numero_particiones);
	free(registro_compaction_time);

	log_warning(logger, "INTENTO BLOQUEAR MEMTABLE EN CREATE");
	pthread_mutex_lock(&mutex_memtable);
	log_warning(logger, "BLOQUEO MEMTABLE EN CREATE");

	// Creamos tabla en memtable
	Tabla* tabla = buscar_tabla(nombre_tabla);
	tabla = crear_tabla(nombre_tabla);
	list_add(memtable, tabla);

	log_info(logger, "La tabla %s se ha creado en la memtable con exito!!", nombre_tabla);

//	log_info(logger, "La tabla %s se ha creado en LFS con exito!!", nombre_tabla);

	log_warning(logger, "INTENTO DESBLOQUEAR MEMTABLE EN CREATE");
	pthread_mutex_unlock(&mutex_memtable);
	log_warning(logger, "DESBLOQUEO MEMTABLE EN CREATE");

	char* mensaje = string_new();
	string_append(&mensaje, "La tabla ");
	string_append(&mensaje, nombre_tabla);
	string_append(&mensaje, " se ha creado con exito en el FS");

	resultado = crear_paquete(CREATE, mensaje);

	free(mensaje);

	pthread_t hilo;
	if (pthread_create(&hilo, &attr, compactar_lfs, (void*) nombre_tabla) < 0) {
		log_error(logger, "No se pudo clear el hilo. OTLA VE.");
		exit(1);
	}

//	mostrar_bitarray();

//	free(nombre_tabla);

	return resultado;

}

void iniciar_memtable() {
	memtable = list_create();
	log_info(logger, "Memtable iniciada.");
}

void eliminar_registro(Registro* registro) {
	free(registro->value);
	free(registro);
}

void eliminar_tabla(Tabla* tabla) {
	for (int i = 0; i < memtable->elements_count; i++) {
		Tabla* t = list_get(memtable, i);
		if (string_equals_ignore_case(t->nombre, tabla->nombre)) {
			list_remove(memtable, i);
			break;
		}
	}
	list_destroy_and_destroy_elements(tabla->registros, (void*) eliminar_registro);
	free(tabla->nombre);
	free(tabla);
//	for (int i = 0; i < memtable->elements_count; i++) {
//		Tabla* t = list_get(memtable, i);
//		if (string_equals_ignore_case(t->nombre, tabla->nombre)) {
//			list_remove(memtable, i);
//			break;
//		}
//	}
}

void limpiar_memtable() {
	list_clean_and_destroy_elements(memtable, (void*) eliminar_tabla);
	log_info(logger, "Memtable limpiada.");
}

void agregar_registro_a_tabla(Tabla* tabla, Registro* registro) {
	list_add(tabla->registros, registro);
}

Tabla* crear_tabla(char* nombre_tabla) {
	Tabla* tabla = malloc(sizeof(Tabla));
	tabla->nombre = string_new();
	string_append(&tabla->nombre, nombre_tabla);
	tabla->registros = list_create();
	return tabla;
}

Registro* crear_registro(uint16_t key, char* value, long timestamp) {
	Registro* registro = malloc(sizeof(Registro));
	registro->key = key;
	registro->timestamp = timestamp;
	registro->value = string_new();
	string_append(&registro->value, value);

	return registro;
}

Tabla* buscar_tabla(char* nombre_tabla) {

	int mismo_nombre(Tabla* tabla) {
		return string_equals_ignore_case(tabla->nombre, nombre_tabla);
	}
	return list_find(memtable, (void*) mismo_nombre);
}

int existe_tabla_en_memtable(char* nombre_tabla) {
	int existe_tabla = 0;
	int i;
	for (i = 0; i < memtable->elements_count; i++) {
		Tabla* tabla = list_get(memtable, i);
		existe_tabla = string_equals_ignore_case(tabla->nombre, nombre_tabla);
		if (existe_tabla != 0)
			break;
	}

	return existe_tabla;
}

Registro* buscar_registro(Tabla* tabla, uint16_t key) {

	int misma_key(Registro* registro) {
		return registro->key == key;
	}
	return list_find(tabla->registros, (void*) misma_key);
}

int guardar_en_memtable(char*nombre_tabla, uint16_t key, char* value, long timestamp) {
	Registro* registro = crear_registro(key, value, timestamp);
	Tabla* tabla = buscar_tabla(nombre_tabla);

	if (tabla == NULL) {
		return 0;
	} else {
		agregar_registro_a_tabla(tabla, registro);
		return 1;
	}
}

char* validar_consistencia(TIPO_CONSISTENCIA consistencia) {
	switch (consistencia) {
	case 1:
		return "SC";
	case 2:
		return "SHC";
	case 3:
		return "EC";
	default:
		return "ERROR";
	}
}

int existe_archivo(char* path_archivo) {
	if (access(path_archivo, F_OK) != -1)
		return 1; // archivo existe

	return 0; // archivo no existe

}

void crear_fichero(char* path) {
	mkdir(path, 0777);
}

char* directorio_de_tablas() {
	char* directorio_tablas = string_new();
	string_append(&directorio_tablas, nombre_punto_montaje);
	string_append(&directorio_tablas, "/tables");
	return directorio_tablas;

}
char* directorio_de_bloques() {
	char* directorio_bloques = string_new();
	string_append(&directorio_bloques, nombre_punto_montaje);
	string_append(&directorio_bloques, "/bloques");
	return directorio_bloques;
}

char* directorio_de_bitmap() {
	char* directorio_bitmap = string_new();
	string_append(&directorio_bitmap, nombre_punto_montaje);
	string_append(&directorio_bitmap, "/Metadata/Bitmap.bin");
	return directorio_bitmap;
}

void inicializar_directorios() {
	directorio_tablas = directorio_de_tablas();
	directorio_bloques = directorio_de_bloques();
	directorio_bitmap = directorio_de_bitmap();
}

void inicializar_nombre_punto_montaje() {
	nombre_punto_montaje = configuracion_lfs->punto_montaje;
}

char* seleccionar_bloque_a_manipular(int numero_de_bloque) {
	char* directorio_bloque_a_manilpular = string_new();
	string_append(&directorio_bloque_a_manilpular, directorio_bloques);
	string_append(&directorio_bloque_a_manilpular, "/");
	char* numero_bloque = string_itoa(numero_de_bloque);
//	sprintf(numero_bloque, "%d", numero_de_bloque);
	string_append(&directorio_bloque_a_manilpular, numero_bloque);
	string_append(&directorio_bloque_a_manilpular, ".bin");
	free(numero_bloque);
	return directorio_bloque_a_manilpular;
}

char* seleccionar_particion_a_manipular(int numero_de_particion, char* directorio_de_tabla_determinada) {
	char* directorio_particion_a_manilpular = string_new();
	string_append(&directorio_particion_a_manilpular, directorio_de_tabla_determinada);
	string_append(&directorio_particion_a_manilpular, "/");
	char* numero_particion = malloc(sizeof(int));
	sprintf(numero_particion, "%d", numero_de_particion);
	string_append(&directorio_particion_a_manilpular, numero_particion);
	string_append(&directorio_particion_a_manilpular, ".bin");
	free(numero_particion);
	return directorio_particion_a_manilpular;
}

void leer_config() {
	configuracion_lfs = malloc(sizeof(Configuracion_LFS));
	configuracion_lfs->ip = string_new();
	configuracion_lfs->puerto = string_new();
	configuracion_lfs->puerto_escucha = string_new();
	configuracion_lfs->punto_montaje = string_new();
	configuracion_lfs->retardo = 0;
	configuracion_lfs->tamanio_value = 0;
	configuracion_lfs->tiempo_dump = 0;

//	t_config* config;
	config = config_create("/home/utnso/Escritorio/tp-2019-1c-dale_que_SO_vo/LFS/configuracion_LFS.config"); //TODO DESHARDCODEAR PATH CONFIG

	string_append(&(configuracion_lfs->ip), config_get_string_value(config, "IP"));
	string_append(&(configuracion_lfs->puerto), config_get_string_value(config, "PUERTO"));
	string_append(&(configuracion_lfs->puerto_escucha), config_get_string_value(config, "PUERTO_ESCUCHA"));
	string_append(&(configuracion_lfs->punto_montaje), config_get_string_value(config, "PUNTO_MONTAJE"));
	configuracion_lfs->retardo = config_get_int_value(config, "RETARDO") / 1000;
	configuracion_lfs->tamanio_value = config_get_int_value(config, "TAMAÑO_VALUE");
	configuracion_lfs->tiempo_dump = config_get_int_value(config, "TIEMPO_DUMP") / 1000;

	log_info(logger, "Ya se puede leer el archivo de configuracion.");

	config_destroy(config);
}

t_list* crear_listado_archivos_en_tablas(char* path_tabla) {
	DIR *directorio;
	struct dirent de, *ent;
	directorio = opendir(path_tabla);
	t_list * listadito = list_create();

	if (directorio == NULL)
		log_error(logger, "No puedo abrir el directorio");
	int cantidadDirectorios = 0;
	while ((readdir_r(directorio, &de, &ent)) == 0 && ent != NULL) {
		if (strcmp(ent->d_name, ".") && strcmp(ent->d_name, "..")) {

			char* nombreDeDirectorio = string_new();
			string_append(&(nombreDeDirectorio), ent->d_name);

			list_add(listadito, nombreDeDirectorio);
			cantidadDirectorios++;
		}
	}

	closedir(directorio);
	return listadito;
}

t_list* crear_listado_nombres_tablas() {
	DIR *directorio;
	struct dirent de, *ent;
	directorio = opendir(directorio_tablas);
	t_list * listadito = list_create();

	if (directorio == NULL)
		log_error(logger, "No puedo abrir el directorio");
	int cantidadDirectorios = 0;
	while ((readdir_r(directorio, &de, &ent)) == 0 && ent != NULL) {
		if (strcmp(ent->d_name, ".") && strcmp(ent->d_name, "..")) {

			char* nombreDeDirectorio = string_new();
			string_append(&(nombreDeDirectorio), ent->d_name);

			list_add(listadito, nombreDeDirectorio);
			cantidadDirectorios++;
		}
	}

	closedir(directorio); //fixme closedir
	return listadito;
}

Paquete* describe(Paquete* paquete) {

	Paquete* resultado;
	char* nombre_enviado = string_new();
	string_append(&nombre_enviado, paquete->buffer->stream);

	char* path_tabla = string_new();
	char* nombre_tabla = nombre_enviado;
	string_append(&path_tabla, directorio_tablas);

	if (strcmp(nombre_tabla, "") != 0) {
//		Verificar que la tabla exista en el file system.
		string_append(&path_tabla, "/");
		string_append(&path_tabla, nombre_tabla);
		if (!existe_carpeta(path_tabla)) {
			free(path_tabla);
			free(nombre_enviado);
			resultado = crear_paquete(DESCRIBE, "No existe la tabla en el file system.\nABORTANDO DESCRIBE..");
			resultado = crear_paquete(DESCRIBE, "No existe la tabla en el file system.\nABORTANDO DESCRIBE..");
			return resultado;
		}
//		Leer el archivo Metadata de dicha tabla.
		string_append(&path_tabla, "/Metadata.bin");

		char registro_metadata[40];
		FILE* archivo_metadata_tabla = fopen(path_tabla, "r");

		int i = 0;
		char array[3][40];
		while (fgets(registro_metadata, 40, (FILE*) archivo_metadata_tabla)) {
			strcpy(array[i], registro_metadata);
			i++;
		}
		fclose(archivo_metadata_tabla);

//	    Retornar el contenido del archivo.
		Datos_describe_tabla* datos_tabla = malloc(sizeof(Datos_describe_tabla));
		datos_tabla->nombre_tabla = string_new();
		datos_tabla->tipo_consistencia = string_new();
		datos_tabla->particiones = string_new();
		datos_tabla->tiempo_compactacion = string_new();

		string_append(&(datos_tabla->nombre_tabla), nombre_tabla);
		string_append(&(datos_tabla->tipo_consistencia), array[0]);
		string_append(&(datos_tabla->particiones), array[1]);
		string_append(&(datos_tabla->tiempo_compactacion), array[2]);

		char** auxiliar_consistencia = string_n_split(datos_tabla->tipo_consistencia, 2, "=");
		char** auxiliar_particiones = string_n_split(datos_tabla->particiones, 2, "=");
		char** auxiliar_tiempo_compactacion = string_n_split(datos_tabla->tiempo_compactacion, 2, "=");

		char* mensaje = string_new();
		string_append(&mensaje, datos_tabla->nombre_tabla);
		string_append(&mensaje, ";");
		char * cadena_aux = quitar_barra_n(auxiliar_consistencia[1]);
		string_append(&mensaje, cadena_aux);
		free(cadena_aux);
		cadena_aux = quitar_barra_n(auxiliar_particiones[1]);
		string_append(&mensaje, ";");
		string_append(&mensaje, cadena_aux);
		free(cadena_aux);
		string_append(&mensaje, ";");
		cadena_aux = quitar_barra_n(auxiliar_tiempo_compactacion[1]);
		string_append(&mensaje, cadena_aux);
		free(cadena_aux);

		resultado = crear_paquete(DESCRIBE, mensaje);

		free(auxiliar_consistencia[0]);
		free(auxiliar_consistencia[1]);
		free(auxiliar_consistencia);

		free(auxiliar_particiones[0]);
		free(auxiliar_particiones[1]);
		free(auxiliar_particiones);

		free(auxiliar_tiempo_compactacion[0]);
		free(auxiliar_tiempo_compactacion[1]);
		free(auxiliar_tiempo_compactacion);

		free(mensaje);
		free(datos_tabla->nombre_tabla);
		free(datos_tabla->tipo_consistencia);
		free(datos_tabla->particiones);
		free(datos_tabla->tiempo_compactacion);
		free(datos_tabla);
		free(path_tabla);
		free(nombre_enviado);

		return resultado;

	} else {
//	Recorrer el directorio de árboles de tablas y descubrir cuales son las tablas que dispone el sistema.

		t_list* nombres_de_directorios = crear_listado_nombres_tablas();

//	Leer los archivos Metadata de cada tabla y retornar el contenido de dichos archivos Metadata.

		char* mensaje = string_new();
		int flagDobleGuion = 1;

		for (int j = 0; j < list_size(nombres_de_directorios); j++) {
			char * path_tabla_evaluada = string_new();
			string_append(&path_tabla_evaluada, directorio_tablas);
			string_append(&path_tabla_evaluada, "/");
			char * nombre_de_tabla_evaluada = list_get(nombres_de_directorios, j);
			string_append(&path_tabla_evaluada, nombre_de_tabla_evaluada);
			string_append(&path_tabla_evaluada, "/Metadata.bin");

			char registro_metadata_evaluado[40];
			FILE* archivo_metadata_tabla_evaluado = fopen(path_tabla_evaluada, "r");

			int i = 0;
			char array[3][40];
			while (fgets(registro_metadata_evaluado, 40, (FILE*) archivo_metadata_tabla_evaluado)) {
				strcpy(array[i], registro_metadata_evaluado);
				i++;
			}
			fclose(archivo_metadata_tabla_evaluado);
			free(path_tabla_evaluada);
			Datos_describe_tabla* datos_tabla_evaluada = malloc(sizeof(Datos_describe_tabla));
			datos_tabla_evaluada->nombre_tabla = string_new();
			datos_tabla_evaluada->tipo_consistencia = string_new();
			datos_tabla_evaluada->particiones = string_new();
			datos_tabla_evaluada->tiempo_compactacion = string_new();

			string_append(&(datos_tabla_evaluada->nombre_tabla), nombre_de_tabla_evaluada);
			free(nombre_de_tabla_evaluada);
			string_append(&(datos_tabla_evaluada->tipo_consistencia), array[0]);
			string_append(&(datos_tabla_evaluada->particiones), array[1]);
			string_append(&(datos_tabla_evaluada->tiempo_compactacion), array[2]);

			char** auxiliar_consistencia = string_n_split(datos_tabla_evaluada->tipo_consistencia, 2, "=");
			char** auxiliar_particiones = string_n_split(datos_tabla_evaluada->particiones, 2, "=");
			char** auxiliar_tiempo_compactacion = string_n_split(datos_tabla_evaluada->tiempo_compactacion, 2, "=");

			string_append(&mensaje, datos_tabla_evaluada->nombre_tabla);
			string_append(&mensaje, ";");
			char * cadena_aux = quitar_barra_n(auxiliar_consistencia[1]);
			string_append(&mensaje, cadena_aux);
			free(cadena_aux);
			cadena_aux = quitar_barra_n(auxiliar_particiones[1]);
			string_append(&mensaje, ";");
			string_append(&mensaje, cadena_aux);
			free(cadena_aux);
			string_append(&mensaje, ";");
			cadena_aux = quitar_barra_n(auxiliar_tiempo_compactacion[1]);
			string_append(&mensaje, cadena_aux);
			free(cadena_aux);

			if (flagDobleGuion < list_size(nombres_de_directorios)) {
				string_append(&mensaje, "--");
				flagDobleGuion++;
			}

			free(datos_tabla_evaluada->nombre_tabla);
			free(datos_tabla_evaluada->tipo_consistencia);
			free(datos_tabla_evaluada->particiones);
			free(datos_tabla_evaluada->tiempo_compactacion);
			free(datos_tabla_evaluada);

			free(auxiliar_consistencia[0]);
			free(auxiliar_consistencia[1]);
			free(auxiliar_consistencia);

			free(auxiliar_particiones[0]);
			free(auxiliar_particiones[1]);
			free(auxiliar_particiones);

			free(auxiliar_tiempo_compactacion[0]);
			free(auxiliar_tiempo_compactacion[1]);
			free(auxiliar_tiempo_compactacion);

		}
		list_destroy(nombres_de_directorios);
		free(path_tabla);

		free(nombre_enviado);

		resultado = crear_paquete(DESCRIBE, mensaje);

		free(mensaje);

		return resultado;
	}
}

char* quitar_barra_n(char* cadena) {
	int longitud_cadena = string_length(cadena);
	char* cadena_cortada = string_substring_until(cadena, longitud_cadena - 1);
	return cadena_cortada;
}

void finalizar() {
//	mostrar_bitarray();
	lfs_encendido = 0;

//	pthread_join(hilo_dump, NULL);
//	pthread_join(hilo_lfs, NULL);
//	pthread_join(hilo_notificador, NULL);
//	pthread_join(hilo_consola, NULL);

	free(bitarray); // se libera memoria de la estructura t_bitarray
	log_destroy(logger);
	list_destroy_and_destroy_elements(memtable, (void*) eliminar_tabla);

//	Liberando config de LFS
	free(configuracion_lfs->ip);
	free(configuracion_lfs->puerto);
	free(configuracion_lfs->puerto_escucha);
	free(configuracion_lfs->punto_montaje);
	free(configuracion_lfs);

//	Liberando punteros a directorios
	free(directorio_tablas);
	free(directorio_bloques);
	free(directorio_bitmap);

//  Libero atributo detached
	pthread_attr_destroy(&attr);

//	Liberando semaforos
	pthread_mutex_destroy(&mutex_ingreso_archivo_config);
	pthread_mutex_destroy(&mutex_bloque);
	pthread_mutex_destroy(&mutex_bitarray);
	pthread_mutex_destroy(&mutex_ingreso_archivo_config);
	pthread_mutex_destroy(&mutex_memtable);
	pthread_mutex_destroy(&mutex_lista_semaforos);
	pthread_mutex_destroy(&mutex_socket);

	exit(1);
}

int devolver_numero_de_primer_bloque_no_ocupado() {
	int j;

	for (j = 0; j < bitarray_get_max_bit(bitarray); j++) {
		if (bitarray_test_bit(bitarray, j) == 0)
			break;
	}
	return j;
}

void mostrar_bitarray() {
	int i;
	bool numero;
	for (i = 0; i < cantidad_bloques_LFS; i++) {
		numero = bitarray_test_bit(bitarray, i);
		log_info(logger, "%d", numero);
	}
}

void iniciar_bitmap(int cant_bloques) {
	bool se_debe_inicializar;
	if (existe_archivo(directorio_bitmap)) {
		log_warning(logger, "Se ha encontrado un archivo Bitmap.bin");
		se_debe_inicializar = false;
	} else {
		log_info(logger, "No existe el archivo Bitmap.bin se procede a inicializarlo...");
		se_debe_inicializar = true;
	}
	int tamanio_bitarray = cant_bloques / 8;
	int fd = open(directorio_bitmap, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
	int result_truncate = ftruncate(fd, tamanio_bitarray);
// char* c_result_truncate = string_itoa(result_truncate);
// log_info(logger, c_result_truncate); //DEBE DEVOLVER CERO SI LO PUDO TRUNCAR

	cadena_bitarray = mmap(NULL, tamanio_bitarray, PROT_READ | PROT_WRITE,
	MAP_SHARED, fd, 0); //SACAR LA DECLARACION DEL .H Y PONERLA ACA CHAR*
	bitarray = bitarray_create_with_mode(cadena_bitarray, tamanio_bitarray, MSB_FIRST);
	if (se_debe_inicializar) {
		//inicio bitarray con ceros
		int a;
		for (a = 0; a < cant_bloques; a++) {
			bitarray_clean_bit(bitarray, a);
			msync(cadena_bitarray, tamanio_bitarray, MS_SYNC);
		}
	}
	log_info(logger, "Estado inicial del bitarray:");
	close(fd);
}

int cantidad_de_bloques_vacios() {
	int i;
	bool numero;
	int contador_de_bloques_vacios = 0;
	for (i = 0; i < cantidad_bloques_LFS; i++) {
		numero = bitarray_test_bit(bitarray, i);
		if (numero) {

		} else {
			contador_de_bloques_vacios++;
		}

	}
	return contador_de_bloques_vacios;

}

Paquete* drop(Paquete* paquete) {

	Paquete* resultado;

	char* nombre_tabla = string_new();
	string_append(&nombre_tabla, paquete->buffer->stream);

	char* path_tabla = string_new();
	char* path_metadata_tabla = string_new();
//Armo path de tabla a borrar
	string_append(&path_tabla, directorio_tablas);
	string_append(&path_tabla, "/");
	string_append(&path_tabla, nombre_tabla);
//Valido si existe la tabla antes de operar
	if (!existe_carpeta(path_tabla)) {
//		log_warning(logger, "No existe la tabla indicada, por tanto no se puede eliminar. \nABORTANDO DROP..");
		free(nombre_tabla);
		free(path_tabla);
		free(path_metadata_tabla);

		resultado = crear_paquete(ERROR, "No existe la tabla indicada, por tanto no se puede eliminar. \nABORTANDO DROP..");
		return resultado;
	}

//Elimino de la memtable para que no se graben nuevos registros //fixme debiera ir semaforo de memtable
	pthread_mutex_lock(&mutex_memtable);
	Tabla* tabla = buscar_tabla(nombre_tabla);
	eliminar_tabla(tabla);
	log_info(logger, "Se ha elminado la tabla de la memtable");
	pthread_mutex_unlock(&mutex_memtable);

//Armo path de la metadata de la tabla a borrar
	string_append(&path_metadata_tabla, path_tabla);
	string_append(&path_metadata_tabla, "/Metadata.bin");

	t_config* metadata_de_tabla = config_create(path_metadata_tabla);
	int cantidad_particiones_tabla = config_get_int_value(metadata_de_tabla, "PARTITIONS");
//Cerramos configuracion de metadata
	config_destroy(metadata_de_tabla);
//Eliminando datos de bloques y actualizando bitarray
	int i;
	for (i = 0; i < cantidad_particiones_tabla; i++) { //recorro todas las particiones
		eliminar_particion_y_vaciar_bloques(path_tabla, i);
	}

//Eliminando temporales, datos de bloque y actualiando bitarray
	int k = 1;
	char* path_archivo_temporal = armar_path_de_archivo_temporal(k, path_tabla);

	while (existe_archivo(path_archivo_temporal)) {
		free(path_archivo_temporal);
		eliminar_temporal_y_vaciar_bloques(path_tabla, k);
		k++;
		path_archivo_temporal = armar_path_de_archivo_temporal(k, path_tabla);
	}

	free(path_archivo_temporal);
	//Eliminamos el resto de archivos
	t_list* archivos_restantes_en_tabla = crear_listado_archivos_en_tablas(path_tabla);
	int cantidad_archivos_restantes = list_size(archivos_restantes_en_tabla);
	for (i = 0; i < cantidad_archivos_restantes; i++) {
		char* nombre_archivo_a_eliminar = list_get(archivos_restantes_en_tabla, i);
		char* path_archivo_a_eliminar = string_new();
		string_append(&path_archivo_a_eliminar, path_tabla);
		string_append(&path_archivo_a_eliminar, "/");
		string_append(&path_archivo_a_eliminar, nombre_archivo_a_eliminar);
		remove(path_archivo_a_eliminar);
		free(nombre_archivo_a_eliminar);
		free(path_archivo_a_eliminar);
	}

	list_destroy(archivos_restantes_en_tabla);
	remove(path_metadata_tabla); //Elimino archivo metadata
	rmdir(path_tabla); //Elimino carpeta de tabla

//	log_info(logger, "Se ha eliminado tabla correctamente, se han vaciado los bloques vinculados a la misma y se han marcado como disponibles para el LFS");
	//Liberamos memoria
	free(path_tabla);
	free(path_metadata_tabla);

	char* mensaje = string_new();
	string_append(&mensaje, "Se ha eliminado la tabla ");
	string_append(&mensaje, nombre_tabla);
	string_append(&mensaje, "  correctamente, se han vaciado los bloques vinculados a la misma y se han marcado como disponibles para el LFS");

	resultado = crear_paquete(DROP, mensaje);

	free(mensaje);
	free(nombre_tabla);

	return resultado;
}

void eliminar_temporal_y_vaciar_bloques(char* path_tabla, int numero_temporal) {
	//Armo path de temporal
	char* numero_de_temporal = string_itoa(numero_temporal);
	char* path_temporal = string_new();
	string_append(&path_temporal, path_tabla);
	string_append(&path_temporal, "/A");
	string_append(&path_temporal, numero_de_temporal);
	string_append(&path_temporal, ".tmp");
	t_config* datos_de_particion = config_create(path_temporal);
	char** numeros_bloques_asociados = config_get_array_value(datos_de_particion, "BLOQUES");
	config_destroy(datos_de_particion);
	int j = 0;
	char* path_bloque;
	int numero_de_bloque;
	while (numeros_bloques_asociados[j] != NULL) {
		sscanf(numeros_bloques_asociados[j], "%d", &numero_de_bloque); //paso el valor del array a int para poder usarlo en la funcion
		path_bloque = seleccionar_bloque_a_manipular(numero_de_bloque);
		vaciar_bloque(path_bloque);			// ACA DEBERIA VACIAR EL BLOQUE
		bitarray_clean_bit(bitarray, numero_de_bloque); // Pongo en cero el bit que indica que el bloque se encontraba ocupado
		free(path_bloque);
		free(numeros_bloques_asociados[j]);
		j++;
	}
	remove(path_temporal); //Elimino los temporales de a uno con la iteracion

	//Libero memoria
	free(numero_de_temporal);
	free(path_temporal);
	free(numeros_bloques_asociados);
}

void eliminar_particion_y_vaciar_bloques(char* path_tabla, int numero_particion) {
	//Armo path de particion
	char* numero_de_particion = string_itoa(numero_particion);
	char* path_particion = string_new();
	string_append(&path_particion, path_tabla);
	string_append(&path_particion, "/");
	string_append(&path_particion, numero_de_particion);
	string_append(&path_particion, ".bin");
	t_config* datos_de_particion = config_create(path_particion);
	char** numeros_bloques_asociados = config_get_array_value(datos_de_particion, "BLOQUES");
	config_destroy(datos_de_particion);
	int j = 0;
	char* path_bloque;
	int numero_de_bloque;
	while (numeros_bloques_asociados[j] != NULL) {
		//	numero_de_bloque = string_atoi(bloques_asociados[j]);  				//paso el valor del array a int para poder usarlo en la funcion- LO DEJO PROVISORIO PORQUE NO EXISTE ATOI
		sscanf(numeros_bloques_asociados[j], "%d", &numero_de_bloque); //paso el valor del array a int para poder usarlo en la funcion
		path_bloque = seleccionar_bloque_a_manipular(numero_de_bloque);
		vaciar_bloque(path_bloque);			// ACA DEBERIA VACIAR EL BLOQUE
		bitarray_clean_bit(bitarray, numero_de_bloque); // Pongo en cero el bit que indica que el bloque se encontraba ocupado
		free(path_bloque);
		free(numeros_bloques_asociados[j]);
		j++;
	}
	remove(path_particion); //Elimino las particiones de a una con la iteracion

	//Libero memoria
	free(numero_de_particion);
	free(path_particion);
	free(numeros_bloques_asociados);
}

void vaciar_bloque(char* path) {
//Elimino bloque
	remove(path);
//Creo bloque vacio
	FILE* archivo_bloque = txt_open_for_append(path);
	txt_close_file(archivo_bloque);
}

void iniciar_compactaciones_de_tablas_existentes() {
	lista_tablas_lfs = crear_listado_nombres_tablas();

	for (int i = 0; i < list_size(lista_tablas_lfs); i++) {
		char* nombre_tabla = list_get(lista_tablas_lfs, i);

		pthread_mutex_lock(&mutex_lista_semaforos);
		iniciar_semaforo(nombre_tabla);
		pthread_mutex_unlock(&mutex_lista_semaforos);

		pthread_t hilo;

		if (pthread_create(&hilo, &attr, compactar_lfs, (void*) nombre_tabla) < 0) {
			log_error(logger, "No se pudo clear el hilo. Chino no come esto cosa.NUNCAA!");
			exit(1);
		}
	}

}

void* compactar_lfs(void* nombre_tabla) {
	char* nombre_tabla_compactar = (char*) nombre_tabla;
//	char* duplicado_nombre_tabla_compactar = string_duplicate(nombre_tabla_compactar);
//	char* nombre_tabla_compactar = "TABLITA2";

	int existe_directorio;

	char* path_tabla = string_new();

	char* path_metadata_tabla = string_new();
	string_append(&path_metadata_tabla, directorio_tablas);
	string_append(&path_metadata_tabla, "/");
	string_append(&path_metadata_tabla, nombre_tabla_compactar);
	//En el medio creo el path de la tabla para validarlo en el while si existe o no
	string_append(&path_tabla, path_metadata_tabla);

	string_append(&path_metadata_tabla, "/Metadata.bin");
	t_config* metadata_tabla = config_create(path_metadata_tabla);
	int tiempo_compactacion = config_get_int_value(metadata_tabla, "COMPACTION_TIME");
	config_destroy(metadata_tabla);
	free(path_metadata_tabla);

	existe_directorio = existe_carpeta(path_tabla);

	while (existe_directorio && lfs_encendido) {
		compactar(nombre_tabla_compactar);
		sleep(tiempo_compactacion);
		existe_directorio = existe_carpeta(path_tabla);
	}

	//protocolar
	free(path_tabla);
	free(nombre_tabla_compactar);
	return NULL;
}

void compactar(char* nombre_tabla_a_compactar) {

	log_warning(logger, "SE INTENTA BLOQUEAR SEMAFORO COMPACTAR PARA TABLA %s", nombre_tabla_a_compactar);
	lock_semaforo_compactar(nombre_tabla_a_compactar);
	log_warning(logger, "SE BLOQUEA SEMAFORO COMPACTAR PARA TABLA %s", nombre_tabla_a_compactar);

	//operatorio de registros
	t_list* lista_registros_de_bloque_particion;
	int cantidad_particiones_de_tabla;
	int se_compacto = 0;   //FLAG PARA VER SI SE COMPACTO LA TABLA O NO

	//------------------------------------------------

//	t_list* nombre_de_tablas_en_lfs = crear_listado_nombres_tablas();   // simplificacion de compactar a una sola tabla
//	int cantidad_tablas_en_lfs = list_size(nombre_de_tablas_en_lfs);
//	int i;

	//ITERANDO POR TABLAS
//	for (i = 0; i < cantidad_tablas_en_lfs; i++) {
//		char* nombre_tabla_a_compactar = list_get(nombre_de_tablas_en_lfs, i);
	char* path_tabla_a_compactar = string_new();
	string_append(&path_tabla_a_compactar, directorio_tablas);
	string_append(&path_tabla_a_compactar, "/");
	string_append(&path_tabla_a_compactar, nombre_tabla_a_compactar);
	//OBTENGO CANTIDAD DE PARTICIONES DE TABLA
	char* path_metadata_de_tabla = string_new(); //
	string_append(&path_metadata_de_tabla, path_tabla_a_compactar);
	string_append(&path_metadata_de_tabla, "/Metadata.bin");
	t_config* configuracion_tabla = config_create(path_metadata_de_tabla);
	cantidad_particiones_de_tabla = config_get_int_value(configuracion_tabla, "PARTITIONS");
	config_destroy(configuracion_tabla);
	free(path_metadata_de_tabla);

	//Renombrar archivos temporales .tmp a .tmpc
	int j = 1;
	char* path_archivo_temporal = armar_path_de_archivo_temporal(j, path_tabla_a_compactar);

	while (existe_archivo(path_archivo_temporal)) {
		//todo ojo semaforo dump, tal vez va
		renombrar_archivos_temporales(path_archivo_temporal);
		free(path_archivo_temporal);
		j++;
		path_archivo_temporal = armar_path_de_archivo_temporal(j, path_tabla_a_compactar);
	}

	log_warning(logger, "SE INTENTA DESBLOQUEAR SEMAFORO COMPACTAR PARA TABLA %s", nombre_tabla_a_compactar);
	unlock_semaforo_compactar(nombre_tabla_a_compactar);
	log_warning(logger, "SE DESBLOQUEA SEMAFORO COMPACTAR PARA TABLA %s", nombre_tabla_a_compactar);

	log_warning(logger, "SE INTENTA BLOQUEAR MUTEX BITARRAY EN COMPACTAR PARA TABLA %s", nombre_tabla_a_compactar);
	pthread_mutex_lock(&mutex_bitarray);
	log_warning(logger, "SE BLOQUEA MUTEX BITARRAY EN COMPACTAR PARA TABLA %s", nombre_tabla_a_compactar);

	//COMIENZO A LEER Y PASAR LOS REGISTROS DEL ARCHIVO TEMPORAL A LA PARTICION
	// ITERANDO POR ACHIVO TEMPORAL DE TABLA ESPECIFICA
	j = 1;
	char* path_archivo_temporal_listo_compactar = armar_path_de_archivo_temporal_listo_compactar(j, path_tabla_a_compactar);

	//			GUARDO LOS REGISTROS DE TODOS LOS BLOQUES DEL ARCHIVO EN LA LISTA
	t_list* lista_registros_de_archivos_temporales = list_create();
	while (existe_archivo(path_archivo_temporal_listo_compactar)) {
		guardar_en_lista_registros_de_archivo(path_archivo_temporal_listo_compactar, lista_registros_de_archivos_temporales, cantidad_particiones_de_tabla);
		//Vaciar bloques de arcihvos temporales y marcar como libres
		t_config* datos_de_archivo_temporal_listo_compactar = config_create(path_archivo_temporal_listo_compactar);
		char** numeros_bloques_asociados = config_get_array_value(datos_de_archivo_temporal_listo_compactar, "BLOQUES");
		config_destroy(datos_de_archivo_temporal_listo_compactar);
		int l = 0;
		int numero_bloque;
		while (numeros_bloques_asociados[l] != NULL) {
			sscanf(numeros_bloques_asociados[l], "%d", &numero_bloque); //paso el valor del array a int para poder usarlo en la funcion
			char* path_bloque = seleccionar_bloque_a_manipular(numero_bloque);
			vaciar_bloque(path_bloque);
			bitarray_clean_bit(bitarray, numero_bloque);
			free(path_bloque);
			free(numeros_bloques_asociados[l]);
			l++;
		}

		free(numeros_bloques_asociados);

		//Elimino archivo temporal
		remove(path_archivo_temporal_listo_compactar);
		free(path_archivo_temporal_listo_compactar);
		j++;
		path_archivo_temporal_listo_compactar = armar_path_de_archivo_temporal_listo_compactar(j, path_tabla_a_compactar);
	}

	log_warning(logger, "SE INTENTA BLOQUEAR MUTEX PARTICION EN COMPACTAR PARA TABLA %s", nombre_tabla_a_compactar);
	lock_semaforo_particion(nombre_tabla_a_compactar);
	log_warning(logger, "SE BLOQUEA MUTEX PARTICION EN COMPACTAR PARA TABLA %s", nombre_tabla_a_compactar);

	// ITERO LA LISTA DE REGISTROS PARA GUARDAR EN PARTICIONES
//	log_error(logger, "Entro al FOR");
//	log_error(logger, "La cantidad de iteracions deberia ser: %d", lista_registros_de_archivos_temporales->elements_count);
	int tam_registro;
	for (j = 0; j < lista_registros_de_archivos_temporales->elements_count; j++) {
//		log_error(logger, "Voy por la iteracion: %d", j);
		t_list* lista_registros_definitivo = list_create();

		Registro_compactacion* registro = list_get(lista_registros_de_archivos_temporales, j);

		//Ver si existe en la particion destino
//		log_error(logger, "Ver si existe en la particion destino");
		if (buscar_key_en_particion(path_tabla_a_compactar, registro->particion_destino, registro->key) == -1) {
			//Si no esta en particion
//			log_error(logger, "Si no esta en particion...");
			char* linea_registro = armar_linea_registro_compactacion(registro);
			char* path_particion_destino = armar_path_particion(path_tabla_a_compactar, registro->particion_destino);
			char* path_bloque_a_escribir = bloque_con_espacio(path_particion_destino, linea_registro);
			//-----------------------------------

			int tamanio_disponible_restante_en_bloque = espacio_disponible_en_bloque(path_bloque_a_escribir);
			FILE* archivo_bloque_a_escribir; // = txt_open_for_append(path_bloque_a_escribir);
			// escribirlo cortado si no hay espacio
			//----------------
			if ((strlen(linea_registro) + 1) <= tamanio_disponible_restante_en_bloque) {
//				log_error(logger, "Cortar registros en IF");
				tamanio_disponible_restante_en_bloque = tamanio_disponible_restante_en_bloque - strlen(linea_registro) - 1;
				archivo_bloque_a_escribir = txt_open_for_append(path_bloque_a_escribir);
				txt_write_in_file(archivo_bloque_a_escribir, linea_registro);
				txt_write_in_file(archivo_bloque_a_escribir, "\n");
				tam_registro = strlen(linea_registro) + 1;
			} else {
//				log_error(logger, "Cortar registros en ELSE");
				string_append(&linea_registro, "\n");
				char* primera_parte_registro_cortado = string_substring_until(linea_registro, tamanio_disponible_restante_en_bloque);
				archivo_bloque_a_escribir = txt_open_for_append(path_bloque_a_escribir);
				txt_write_in_file(archivo_bloque_a_escribir, primera_parte_registro_cortado);
				txt_close_file(archivo_bloque_a_escribir);
				free(path_bloque_a_escribir);
				path_bloque_a_escribir = aniadir_bloque_asignado(path_particion_destino);
				char* segunda_parte_registro_cortado = string_substring_from(linea_registro, tamanio_disponible_restante_en_bloque);
				tamanio_disponible_restante_en_bloque = tamanio_bloque_LFS - strlen(segunda_parte_registro_cortado);
				archivo_bloque_a_escribir = txt_open_for_append(path_bloque_a_escribir);
				txt_write_in_file(archivo_bloque_a_escribir, segunda_parte_registro_cortado);
				tam_registro = strlen(linea_registro);
				free(segunda_parte_registro_cortado);
				free(primera_parte_registro_cortado);
			}

			//-------------
			t_config* config_particion = config_create(path_particion_destino);
			int tamanio_particion = config_get_int_value(config_particion, "SIZE");
			tamanio_particion = tamanio_particion + tam_registro;
			char* char_tamanio_particion = string_itoa(tamanio_particion);
			config_set_value(config_particion, "SIZE", char_tamanio_particion);
			config_save(config_particion);
			config_destroy(config_particion);
			free(char_tamanio_particion);
			txt_close_file(archivo_bloque_a_escribir);
			free(linea_registro);
			free(path_bloque_a_escribir);
			free(path_particion_destino);
			//----------------------
		} else {
//			SI LA KEY ESTA REPETIDA..
//			int numero_bloque_donde_esta_la_key = buscar_key_en_particion(path_tabla_a_compactar, registro->particion_destino, registro->key);
//			char* path_bloque_destino = seleccionar_bloque_a_manipular(numero_bloque_donde_esta_la_key);
//			char* path_particion_destino = seleccionar_particion_a_manipular(registro->particion_destino, path_tabla_a_compactar);
//			lista_registros_de_bloque_particion = leer_datos_de_bloque(path_bloque_destino);
			//Craglo en la list atodos los registros de la praticion
//			log_error(logger, "Si la key esta repetida...");
			lista_registros_de_bloque_particion = leer_datos_de_particion(path_tabla_a_compactar, registro->particion_destino);
			int k;
			int tamanio_anterior = 0;
			int tamanio_nuevo = 0;
//			log_error(logger, "Entro al FOR de bloque de particiones");
//			log_error(logger, "Cantidad de iteracions de particiones. %d", lista_registros_de_bloque_particion->elements_count);
			for (k = 0; k < lista_registros_de_bloque_particion->elements_count; k++) {
//				log_error(logger, "iteracion K numero: %d", k);
				Registro_compactacion* registro_particion = list_get(lista_registros_de_bloque_particion, k); //char* linea_registro
				tamanio_anterior += registro_particion->tamanio_registro;

				if (registro->key == registro_particion->key) {
					// para la key igual comparo timestamp y pongo la mayor
					if (registro->timestamp > registro_particion->timestamp) {
						list_add(lista_registros_definitivo, registro);
						tamanio_nuevo += registro->tamanio_registro;
					} else {
						//Si es la que estaba tenia el timestamp mayor
						list_add(lista_registros_definitivo, registro_particion);
						tamanio_nuevo += registro_particion->tamanio_registro;
					}

				} else {
					//Para el resto de las key las agrego
					list_add(lista_registros_definitivo, registro_particion);
					tamanio_nuevo += registro_particion->tamanio_registro;
				}

			}
			char* path_particion_destino = seleccionar_particion_a_manipular(registro->particion_destino, path_tabla_a_compactar);
			//Escribo el bloque con los registros de la lista definitiva
//			lock_semaforo_particion(nombre_tabla_a_compactar); fixme lo borro porque puse mutex adentro

//			log_error(logger, "Comienzo a escribir en particion");
			escribir_lista_registros_en_particion(lista_registros_definitivo, path_particion_destino);
//			unlock_semaforo_particion(nombre_tabla_a_compactar); fixme lo borro porque puse mutex adentor
//			log_error(logger, "Termine de escribir la particion");
			//Modificar tamaño de la particion segun corresponda

			//----------------------------------------------
			free(path_particion_destino);
			list_destroy_and_destroy_elements(lista_registros_de_bloque_particion, (void*) eliminar_registro_compactacion);

		}

//		log_error(logger, "Destruyendo lista de registros definitivos");
		list_destroy(lista_registros_definitivo);
		se_compacto = 1; // se indica que compacto
	}

//	log_info(logger, "Se evalua si se va a compactar o no");
	if (se_compacto) {
		log_info(logger, "Se ha compactado la tabla %s", nombre_tabla_a_compactar);
	} else {
		log_info(logger, "No hay datos para compactar para la tabla %s", nombre_tabla_a_compactar);
	}

//	log_info(logger, "Libero cosas");
	free(path_archivo_temporal);
	free(path_tabla_a_compactar);
//		free(nombre_tabla_a_compactar); //fixme a mi entender no se deberia liberar, probar y revisar
	free(path_archivo_temporal_listo_compactar);
	list_destroy_and_destroy_elements(lista_registros_de_archivos_temporales, (void*) eliminar_registro_compactacion);

	log_warning(logger, "SE INTENTA DESBLOQUEAR MUTEX PARTICION EN COMPACTAR PARA TABLA %s", nombre_tabla_a_compactar);
	unlock_semaforo_particion(nombre_tabla_a_compactar);
	log_warning(logger, "SE DESBLOQUEA MUTEX PARTICION EN COMPACTAR PARA TABLA %s", nombre_tabla_a_compactar);

	log_warning(logger, "SE INTENTA DESBLOQUEAR MUTEX BITARRAY PARA TABLA %s EN COMPACTAR", nombre_tabla_a_compactar);
	pthread_mutex_unlock(&mutex_bitarray);
	log_warning(logger, "SE DESBLOQUEA MUTEX BITARRAY EN COMPACTAR PARA TABLA %s", nombre_tabla_a_compactar);
//	}
//	list_destroy(nombre_de_tablas_en_lfs);

}

t_list* leer_datos_de_particion(char* path_tabla, int numero_particion) {
	t_list* lista_registros = list_create();
	Registro_compactacion* registro;
	int numero_bloque;
	char* stream_registros_bloque;
	char* stream_registros_total = string_new();

	//ARMO PATH PARTICOIN
	char* path_particion = armar_path_particion(path_tabla, numero_particion);

	//OBTENGO BLOQUES ASOCIADOS A LA PARTICION
	t_config* datos_de_particion = config_create(path_particion);
	char** numeros_bloques_asociados = config_get_array_value(datos_de_particion, "BLOQUES");
	config_destroy(datos_de_particion);

	//RECORRO BLOQUES DE PARTICION
	int i = 0;
	while (numeros_bloques_asociados[i] != NULL) {
		sscanf(numeros_bloques_asociados[i], "%d", &numero_bloque); //paso el valor del array a int para poder usarlo en la funcion
		char* path_bloque = seleccionar_bloque_a_manipular(numero_bloque);
		stream_registros_bloque = leer_datos_de_bloque(path_bloque);
		string_append(&stream_registros_total, stream_registros_bloque);
		free(numeros_bloques_asociados[i]);
		free(stream_registros_bloque);
		free(path_bloque);
		i++;
	}

	char** registros_separados = string_split(stream_registros_total, "\n");
	int j = 0;
	while (registros_separados[j] != NULL) {
		registro = deserializar_registro(registros_separados[j]);
		list_add(lista_registros, registro);
		free(registros_separados[j]);
		j++;
	}

	free(path_particion);
	free(registros_separados);
	free(stream_registros_total);
	free(numeros_bloques_asociados);
	return lista_registros;
}

int buscar_key_en_particion(char* path_tabla, int numero_particion, uint16_t key) {
	t_list* lista_registros = list_create();
	Registro_compactacion* registro;
	int numero_bloque;
	char* stream_registros_bloque;
	char* stream_registros_total = string_new();

	char* path_particion = armar_path_particion(path_tabla, numero_particion);
	int bloque_de_la_key = -1;
	//OBTENGO BLOQUES ASOCIADOS A LA PARTICION
	t_config* datos_de_particion = config_create(path_particion);
	char** numeros_bloques_asociados = config_get_array_value(datos_de_particion, "BLOQUES");
	config_destroy(datos_de_particion);

	int i = 0;
	while (numeros_bloques_asociados[i] != NULL) {
		sscanf(numeros_bloques_asociados[i], "%d", &numero_bloque); //paso el valor del array a int para poder usarlo en la funcion
		char* path_bloque = seleccionar_bloque_a_manipular(numero_bloque);
		stream_registros_bloque = leer_datos_de_bloque(path_bloque);
		string_append(&stream_registros_total, stream_registros_bloque);
		free(numeros_bloques_asociados[i]);
		free(stream_registros_bloque);
		free(path_bloque);
		i++;
	}
	//Del stream spliteamos por \n y armamos lista de registros
	char** registros_separados = string_split(stream_registros_total, "\n");
	int j = 0;
	while (registros_separados[j] != NULL) {
		registro = deserializar_registro(registros_separados[j]);
		list_add(lista_registros, registro);
		free(registros_separados[j]);
		j++;
	}
	free(registros_separados);

	//BUSCO EN LA LISTA DE REGISTROS LA KEY
	int k = 0;
	while (bloque_de_la_key == -1 && k < list_size(lista_registros)) {
		registro = list_get(lista_registros, k);
		if (registro->key == key) {
			bloque_de_la_key = 1;
		}
		k++;
	}

	list_destroy_and_destroy_elements(lista_registros, (void*) eliminar_registro_compactacion);

	free(stream_registros_total);
	free(numeros_bloques_asociados);
	free(path_particion);
	return bloque_de_la_key;
}

char* armar_path_particion(char* path_tabla, int numero_particion) {
	char* path_particion = string_new();
	char* char_numero_particion = string_itoa(numero_particion);
	string_append(&path_particion, path_tabla);
	string_append(&path_particion, "/");
	string_append(&path_particion, char_numero_particion);
	string_append(&path_particion, ".bin");

	free(char_numero_particion);
	return path_particion;
}

void renombrar_archivos_temporales(char* path_viejo) {
	char* path_renombrado = string_new();
	string_append(&path_renombrado, path_viejo);
	string_append(&path_renombrado, "c");
	rename(path_viejo, path_renombrado);
	free(path_renombrado);
}

char* armar_path_de_archivo_temporal(int numero_archivo_temporal, char* path_tabla) {
	char* path_archivo_temporal = string_new();
	char* char_numero_archivo_temporal = string_itoa(numero_archivo_temporal);
	string_append(&path_archivo_temporal, path_tabla);
	string_append(&path_archivo_temporal, "/A");
	string_append(&path_archivo_temporal, char_numero_archivo_temporal);
	string_append(&path_archivo_temporal, ".tmp");
	free(char_numero_archivo_temporal);
	return path_archivo_temporal;
}

char* armar_path_de_archivo_temporal_listo_compactar(int numero_archivo_temporal, char* path_tabla) {
	char* path_archivo_temporal = string_new();
	char* char_numero_archivo_temporal = string_itoa(numero_archivo_temporal);
	string_append(&path_archivo_temporal, path_tabla);
	string_append(&path_archivo_temporal, "/A");
	string_append(&path_archivo_temporal, char_numero_archivo_temporal);
	string_append(&path_archivo_temporal, ".tmpc");
	free(char_numero_archivo_temporal);
	return path_archivo_temporal;
}

void guardar_en_lista_registros_de_archivo(char* path_archivo_particion_o_temporal, t_list* lista_registros, int cantidad_particiones) { //agrega a la lista los registros de un archivo
	// LEO QUE BLOQUES ESTAN ASOCIADOS A LA PARTICION O ARCHIVO TEMPORAL
	t_config* datos_de_archivo_temporal_listo_compactar = config_create(path_archivo_particion_o_temporal);
	char** numeros_bloques_asociados = config_get_array_value(datos_de_archivo_temporal_listo_compactar, "BLOQUES");
	config_destroy(datos_de_archivo_temporal_listo_compactar);
	int k = 0;
	int numero_bloque;
	char* stream_registros_bloque;
	char* stream_registros_total = string_new();
	Registro_compactacion* registro;

	//ITERO POR BLOQUES
	while (numeros_bloques_asociados[k] != NULL) {
		sscanf(numeros_bloques_asociados[k], "%d", &numero_bloque); //paso el valor del array a int para poder usarlo en la funcion
		char* path_bloque = seleccionar_bloque_a_manipular(numero_bloque);
		stream_registros_bloque = leer_datos_de_bloque(path_bloque);
		string_append(&stream_registros_total, stream_registros_bloque);
		free(stream_registros_bloque);
		free(path_bloque);
		free(numeros_bloques_asociados[k]);
		k++;
	}

	char** registros_separados = string_split(stream_registros_total, "\n");
	int j = 0;
	while (registros_separados[j] != NULL) {
		registro = deserializar_registro_temporal(registros_separados[j], cantidad_particiones);
		list_add(lista_registros, registro);
		free(registros_separados[j]);
		j++;
	}

	free(registros_separados);
	free(stream_registros_total);
	free(numeros_bloques_asociados);
	return;
}

char* leer_datos_de_bloque(char* path_bloque) { //devuelve stream de registros de un bloque
	FILE* bloque_con_datos_temporal = fopen(path_bloque, "r");
	char* stream_registros = string_new();
//	t_list* registros_compactacion = list_create();
	if (bloque_con_datos_temporal == NULL) {
		perror("No se pueden leer registros del bloque");
		exit(1);
	}
	char buffer[128]; //FIXME reevaluar tamaño del buffer -- EL buffer es por linea

	// revisar si va o no (CONTINGENCIA ARCHIVO VACIO)

	while (fgets(buffer, sizeof(buffer), bloque_con_datos_temporal) != NULL) {
		string_append(&stream_registros, buffer);
	}

	fclose(bloque_con_datos_temporal);
	return stream_registros;
}

Registro_compactacion* deserializar_registro_temporal(char* linea, int cantidad_particiones) {
	Registro_compactacion* registro_compactacion = malloc(sizeof(Registro_compactacion));
	char** linea_deserializada = string_n_split(linea, 3, ";");

//	log_error(logger, "El timestamp es %s", linea_deserializada[0]);
//	log_error(logger, "La key es %s", linea_deserializada[1]);
//	log_error(logger, "EL value es %s", linea_deserializada[2]); // fixme SACAR DE ACA PLIS

	long timestamp = atol(linea_deserializada[0]);
	uint16_t key = atoi(linea_deserializada[1]);
	char* value;
	if (string_ends_with(linea_deserializada[2], "\n")) {
		value = string_substring_until(linea_deserializada[2], (strlen(linea_deserializada[2]) - 1));
	} else {
		value = linea_deserializada[2];
	}
	int particion = determinar_particion(key, cantidad_particiones);

	registro_compactacion->timestamp = timestamp;
	registro_compactacion->key = key;
	registro_compactacion->value = value;
	registro_compactacion->tamanio_registro = strlen(linea) - 1;
	registro_compactacion->particion_destino = particion;

	free(linea_deserializada[0]);
	free(linea_deserializada[1]);
//	free(linea_deserializada[2]);
	free(linea_deserializada);

	return registro_compactacion;
}

Registro_compactacion* deserializar_registro(char* linea) {
	Registro_compactacion* registro_compactacion = malloc(sizeof(Registro_compactacion));
	char** linea_deserializada = string_n_split(linea, 3, ";");

//	log_error(logger, "El timestamp es %s", linea_deserializada[0]);
//	log_error(logger, "La key es %s", linea_deserializada[1]);
//	log_error(logger, "EL value es %s", linea_deserializada[2]); // fixme SACAR DE ACA PLIS

	long timestamp = atol(linea_deserializada[0]);
	uint16_t key = atoi(linea_deserializada[1]);
//	char* value =  linea_deserializada[2];
//	char* value = string_substring_until(linea_deserializada[2], (strlen(linea_deserializada[2])-1) );
	char* value;
	if (string_ends_with(linea_deserializada[2], "\n")) {
		value = string_substring_until(linea_deserializada[2], (strlen(linea_deserializada[2]) - 1));
	} else {
		value = linea_deserializada[2];
	}

	registro_compactacion->timestamp = timestamp;
	registro_compactacion->key = key;
	registro_compactacion->value = value;
	registro_compactacion->tamanio_registro = strlen(linea) - 1;

	free(linea_deserializada[0]);
	free(linea_deserializada[1]);
//	free(linea_deserializada[2]);
	free(linea_deserializada);

	return registro_compactacion;
}

Paquete* insert(Paquete* paquete_peticion) {
	char** auxiliar = string_n_split(paquete_peticion->buffer->stream, 3, " ");

	char* nombre_tabla = string_new();
	string_append(&nombre_tabla, auxiliar[0]);
	uint16_t key = atoi(auxiliar[1]);
	char* valor = string_new();
	long timestamp;

	char** separar;
	if (string_ends_with(auxiliar[2], "\"")) {
		char* asd = string_substring(auxiliar[2], 1, strlen(auxiliar[2]) - 2); //le saco las comillas cuando entro por este lado
		string_append(&valor, asd);
		free(asd);

		timestamp = (long) time(NULL);
	} else {
		separar = string_split(auxiliar[2], "\"");
		string_append(&valor, separar[0]);
		timestamp = (long) atoi(separar[1]);

		for (int i = 0; separar[i] != NULL; i++)
			free(separar[i]);
		free(separar);
	}

	for (int i = 0; auxiliar[i] != NULL; i++)
		free(auxiliar[i]);
	free(auxiliar);

	Paquete* respuesta;

	if (configuracion_lfs->tamanio_value < strlen(valor)) {
		log_error(logger, "El value puede tener como maximo %d digitos", configuracion_lfs->tamanio_value);
		respuesta = crear_paquete(ERROR, "El value es muy grande");
		return respuesta;
	}

	log_warning(logger, "INTENTO BLOQUEAR MEMTABLE EN INSERT");
	pthread_mutex_lock(&mutex_memtable);
	log_warning(logger, "BLOQUEO MEMTABLE EN INSERT");
	if (existe_tabla_en_memtable(nombre_tabla)) {

		guardar_en_memtable(nombre_tabla, key, valor, timestamp);

		log_info(logger, "El registro se ha insertado en la memtable");
		respuesta = crear_paquete(INSERT, "El registro se ha insertado con exito");
	} else {
		log_error(logger, "No existe la tabla %s en la memtable, por lo tanto no se puede guardar registro", nombre_tabla);
		respuesta = crear_paquete(ERROR, "No existe la tabla! No se logro insertar");
	}
	free(nombre_tabla);
	free(valor);

	log_warning(logger, "INTENTO DESBLOQUEAR MEMTABLE EN INSERT");
	pthread_mutex_unlock(&mutex_memtable);
	log_warning(logger, "DESBLOQUEO MEMTABLE EN INSERT");

	return respuesta;
}

void dump() {
	int i;
	char* path_bloque_a_escribir;
	Registro* registro;
	Tabla* tabla_en_memtable;
	char* linea_registro;
	char* path_archivo_temporal;
	FILE* archivo_temporal_a_escribir;
	FILE* archivo_bloque_a_escribir;

	t_list* nombre_de_tablas_en_lfs = crear_listado_nombres_tablas();
	int cantidad_tablas_en_lfs = list_size(nombre_de_tablas_en_lfs);
	int j;
	for (j = 0; j < cantidad_tablas_en_lfs; j++) {

		char* nombre_tabla_dumpear = list_get(nombre_de_tablas_en_lfs, j);
		log_warning(logger, "voy a dumpear tabla %s", nombre_tabla_dumpear);

		log_warning(logger, "INTENTO BLOQUEAR MEMTABLE EN DUMP");
		pthread_mutex_lock(&mutex_memtable);
		log_warning(logger, "BLOQUEO MEMTABLE EN DUMP");
		log_warning(logger, "INTENTO BLOQUEAR COMPACTAR TABLA %s EN DUMP", nombre_tabla_dumpear);
		lock_semaforo_compactar(nombre_tabla_dumpear);
		log_warning(logger, "BLOQUEO COMPACTAR TABLA %s EN DUMP", nombre_tabla_dumpear);

		tabla_en_memtable = buscar_tabla(nombre_tabla_dumpear);

		// por tabla -> iteracoin

		//VALIDAR SI LA TABLA TIENE REGISTROS PARA DUMPEAR. SINO SALTA
		if (tabla_en_memtable->registros->elements_count == 0) {
			log_warning(logger, "La tabla %s no tiene registros para dumpear", nombre_tabla_dumpear);
//			free(nombre_tabla_dumpear);
		} else {
			if (!cantidad_de_bloques_vacios()) {
				log_warning(logger, "No hay bloques vacios para asignar al archivo temporal de la tabla %s, la misma no se puede dumpear", nombre_tabla_dumpear);
				free(nombre_tabla_dumpear);

				unlock_semaforo_compactar(nombre_tabla_dumpear);
				pthread_mutex_unlock(&mutex_memtable);

				continue;
			}
			//VALIDAR SI HAY ESPACIOS PARA DUMPEAR TODA LA TABLA
			log_warning(logger, "INTENTO BLOQUEAR BITARRAY EN DUMP");
			pthread_mutex_lock(&mutex_bitarray);
			log_warning(logger, "BLOQUEO BITARRAY EN DUMP");

			if (hay_espacio_para_dumpear_toda_la_tabla(tabla_en_memtable) == 0) {
				log_warning(logger, "No hay espacio suficiente en bloques para que la tabla %s haga su dump", nombre_tabla_dumpear);
				free(nombre_tabla_dumpear);

				pthread_mutex_unlock(&mutex_bitarray);
				unlock_semaforo_compactar(nombre_tabla_dumpear);
				pthread_mutex_unlock(&mutex_memtable);

				continue;
			}
			//Armo path de tabla a dumpear
			char* path_tabla_a_dumpear = string_new();
			string_append(&path_tabla_a_dumpear, directorio_tablas);
			string_append(&path_tabla_a_dumpear, "/");
			string_append(&path_tabla_a_dumpear, nombre_tabla_dumpear);

			log_error(logger, "la tabla %s empieza a dumpear", nombre_tabla_dumpear); //fixme sacar

			//Crear archivo temporal
			//numero_archivo_temporal
			int numero_archivo_temporal = 1;
			path_archivo_temporal = armar_path_de_archivo_temporal(numero_archivo_temporal, path_tabla_a_dumpear);
			while (existe_archivo(path_archivo_temporal)) {
				free(path_archivo_temporal);
				numero_archivo_temporal++;
				path_archivo_temporal = armar_path_de_archivo_temporal(numero_archivo_temporal, path_tabla_a_dumpear);
			}

			archivo_temporal_a_escribir = txt_open_for_append(path_archivo_temporal);

			txt_write_in_file(archivo_temporal_a_escribir, "SIZE=0\nBLOQUES=[");
			int numero_de_primer_bloque_no_ocupado = devolver_numero_de_primer_bloque_no_ocupado();
			char* char_numero_de_primer_bloque_no_ocupado = string_itoa(numero_de_primer_bloque_no_ocupado);
			txt_write_in_file(archivo_temporal_a_escribir, char_numero_de_primer_bloque_no_ocupado);
			txt_write_in_file(archivo_temporal_a_escribir, "]");
			txt_close_file(archivo_temporal_a_escribir);

			bitarray_set_bit(bitarray, numero_de_primer_bloque_no_ocupado);

			path_bloque_a_escribir = seleccionar_bloque_a_manipular(numero_de_primer_bloque_no_ocupado); //AGREGADO
			int tamanio_dump = 0; //AGREGADO
			int tamanio_disponible_restante_en_bloque = tamanio_bloque_LFS;

			t_list* tabla_filtrada = crear_lista_sin_registros_repetidos(tabla_en_memtable->registros);

			for (i = 0; i < tabla_filtrada->elements_count; i++) {
				registro = list_get(tabla_filtrada, i);
				linea_registro = armar_linea_registro(registro);
				tamanio_dump += strlen(linea_registro) + 1;
				if ((strlen(linea_registro) + 1) <= tamanio_disponible_restante_en_bloque) {
					tamanio_disponible_restante_en_bloque = tamanio_disponible_restante_en_bloque - strlen(linea_registro) - 1;
					archivo_bloque_a_escribir = txt_open_for_append(path_bloque_a_escribir);
					txt_write_in_file(archivo_bloque_a_escribir, linea_registro);
					txt_write_in_file(archivo_bloque_a_escribir, "\n");

//					log_error(logger, "grabo %s entero", linea_registro); //fixme sacar

				} else {
					string_append(&linea_registro, "\n");
					char* primera_parte_registro_cortado = string_substring_until(linea_registro, tamanio_disponible_restante_en_bloque);
					archivo_bloque_a_escribir = txt_open_for_append(path_bloque_a_escribir);
					txt_write_in_file(archivo_bloque_a_escribir, primera_parte_registro_cortado);
					txt_close_file(archivo_bloque_a_escribir);
//					log_error(logger, "grabo %s primera parte en bloque %s", primera_parte_registro_cortado, path_bloque_a_escribir); //fixme sacar
					free(path_bloque_a_escribir);
					path_bloque_a_escribir = aniadir_bloque_asignado(path_archivo_temporal);
					char* segunda_parte_registro_cortado = string_substring_from(linea_registro, tamanio_disponible_restante_en_bloque);
					tamanio_disponible_restante_en_bloque = tamanio_bloque_LFS - strlen(segunda_parte_registro_cortado);
					archivo_bloque_a_escribir = txt_open_for_append(path_bloque_a_escribir);
					txt_write_in_file(archivo_bloque_a_escribir, segunda_parte_registro_cortado);

//					log_error(logger, "grabo %s segunda parte en bloque %s", segunda_parte_registro_cortado, path_bloque_a_escribir); //fixme sacar

					free(segunda_parte_registro_cortado);
					free(primera_parte_registro_cortado);
				}

				//------------------------------------------- char* aniadir_bloque_asignado(char* path_particion_o_temporal)

				txt_close_file(archivo_bloque_a_escribir);
				free(linea_registro);

			}
			free(path_bloque_a_escribir);

			char* char_tamanio_archivo_temporal = string_itoa(tamanio_dump);

			t_config* config_archivo_temporal = config_create(path_archivo_temporal);
			config_set_value(config_archivo_temporal, "SIZE", char_tamanio_archivo_temporal);
			config_save(config_archivo_temporal);
			config_destroy(config_archivo_temporal);

			list_clean_and_destroy_elements(tabla_en_memtable->registros, (void*) eliminar_registro);

			log_info(logger, "La tabla %s ha hecho un dump", nombre_tabla_dumpear);
			//------------
			free(path_archivo_temporal);
			free(char_tamanio_archivo_temporal);
			free(char_numero_de_primer_bloque_no_ocupado);

			free(path_tabla_a_dumpear);
			list_destroy(tabla_filtrada);

			log_warning(logger, "INTENTO DESBLOQUEAR BITARRAY");
			pthread_mutex_unlock(&mutex_bitarray);
			log_warning(logger, "DESBLOQUEO BITARRAY ");

		}
		log_warning(logger, "INTENTO DESBLOQUEAR COMPACTAR Y MEMTABLE ");

		unlock_semaforo_compactar(nombre_tabla_dumpear);
		pthread_mutex_unlock(&mutex_memtable);
		log_warning(logger, "DESBLOQUEO COMPACTAR Y MEMTABLE ");
		free(nombre_tabla_dumpear);

	}
	list_destroy(nombre_de_tablas_en_lfs);
}

void* dump_lfs() {

	while (lfs_encendido) {

		sleep(configuracion_lfs->tiempo_dump);
		dump();
	}
	return NULL;
}

char* armar_linea_registro(Registro* registro) {
	char* linea_registro = string_new();
	char* timestamp_string = string_itoa(registro->timestamp);
	char* key_string = string_itoa(registro->key);
	string_append(&linea_registro, timestamp_string);
	string_append(&linea_registro, ";");
	string_append(&linea_registro, key_string);
	string_append(&linea_registro, ";");
	string_append(&linea_registro, registro->value);
	free(timestamp_string);
	free(key_string);
	return linea_registro;
}

char* armar_linea_registro_compactacion(Registro_compactacion* registro) {
	char* linea_registro = string_new();
	char* timestamp_string = string_itoa(registro->timestamp);
	char* key_string = string_itoa(registro->key);
	string_append(&linea_registro, timestamp_string);
	string_append(&linea_registro, ";");
	string_append(&linea_registro, key_string);
	string_append(&linea_registro, ";");
	string_append(&linea_registro, registro->value);
	free(timestamp_string);
	free(key_string);
	return linea_registro;
}

int determinar_particion(int numero_key, int cantidad_particiones) {
	int particion = numero_key % cantidad_particiones;
	return particion;
}

void leer_metadata_LFS() {

	char* carpeta_metadata = string_new();
	char* metadata_bin = string_new();

	string_append(&carpeta_metadata, nombre_punto_montaje);
	string_append(&carpeta_metadata, "/Metadata");
	string_append(&metadata_bin, carpeta_metadata);
	string_append(&metadata_bin, "/Metadata.bin");
	log_info(logger, "%s", metadata_bin);
	t_config* datos_de_metadata = config_create(metadata_bin);

	cantidad_bloques_LFS = config_get_int_value(datos_de_metadata, "BLOCKS");
	tamanio_bloque_LFS = config_get_int_value(datos_de_metadata, "BLOCK_SIZE");
	magic_number = config_get_string_value(datos_de_metadata, "MAGIC_NUMBER");

	config_destroy(datos_de_metadata);
	free(metadata_bin);
	free(carpeta_metadata);

}

//void select_lfs(char* nombre_tabla, uint16_t key){ / elminar solo para probarlo
Paquete* select_lfs(Paquete* paquete) {
	Paquete* resultado;
	char** auxiliar = string_n_split(paquete->buffer->stream, 2, " ");

	char* nombre_tabla = string_new();
	string_append(&nombre_tabla, auxiliar[0]);
	uint16_t key = atoi(auxiliar[1]);

	free(auxiliar[0]);
	free(auxiliar[1]);
	free(auxiliar);

	int encontrada_en_particion = 0;
	int encontrada_en_temporal = 0;
	int encontrada_en_memtable = 0;

	//verificar que la tabla exista en el file system
	char* path_tabla = string_new();
	string_append(&path_tabla, directorio_tablas);
	string_append(&path_tabla, "/");
	string_append(&path_tabla, nombre_tabla);

	if (existe_carpeta(path_tabla)) {
		log_info(logger, "Existe la tabla en el file system");
	} else {
//		log_info(logger, "No existe la tabla en el file system");
		resultado = crear_paquete(ERROR, "No existe la tabla en el filesystem");
		free(path_tabla);
		free(nombre_tabla);
		return resultado;
	}

	//obtener metadata
	char* path_tabla_metadata = string_new();
	string_append(&path_tabla_metadata, path_tabla);
	string_append(&path_tabla_metadata, "/Metadata.bin");

	t_config* config_metadata_tabla = config_create(path_tabla_metadata);
	int cant_particiones = config_get_int_value(config_metadata_tabla, "PARTITIONS");
	config_destroy(config_metadata_tabla);
	free(path_tabla_metadata);

	//calcular particion
	int numero_de_particion = determinar_particion(key, cant_particiones);

	Registro_compactacion* registro_de_particion;
	t_list* registros_de_particion;

	Registro* registro_de_memtable;

	Registro_compactacion* registro_de_temporal;
	t_list* lista_registros_de_archivos_temporales;

	//BUSCAMOS LA KEY
	int i;

	log_warning(logger, "SE INTENTA BLOQUEAR MUTEX COMPACTAR EN SELECT PARA TABLA %s", nombre_tabla);
	lock_semaforo_compactar(nombre_tabla); //fixme sem particioN
	log_warning(logger, "SE BLOQUEA MUTEX COMPACTAR EN SELECT PARA TABLA %s", nombre_tabla);
	log_warning(logger, "SE INTENTA BLOQUEAR MUTEX PARTICION EN SELECT PARA TABLA %s", nombre_tabla);
	lock_semaforo_particion(nombre_tabla); //fixme sem particion
	log_warning(logger, "SE BLOQUEA MUTEX PARTICION EN SELECT PARA TABLA %s", nombre_tabla);

	//Busco en que bloque esta la key
	int bloque_de_la_key = buscar_key_en_particion(path_tabla, numero_de_particion, key);

	int esta_en_particion = 0;
	if (bloque_de_la_key != -1) {
		//Busco en el bloque la key
		esta_en_particion = 1;

		registros_de_particion = leer_datos_de_particion(path_tabla, numero_de_particion);

		for (i = 0; i < registros_de_particion->elements_count; i++) {
			registro_de_particion = list_get(registros_de_particion, i);
			if (key == registro_de_particion->key) {
				encontrada_en_particion = 1;
				break;
			}
		}
	}

	log_warning(logger, "SE INTENTA DESBLOQUEAR MUTEX PARTICION EN SELECT PARA TABLA %s", nombre_tabla);
	unlock_semaforo_particion(nombre_tabla); //fixme sem particion
	log_warning(logger, "SE DESBLOQUEA MUTEX PARTICION EN SELECT PARA TABLA %s", nombre_tabla);
	log_warning(logger, "SE INTENTA DESBLOQUEAR MUTEX COMPACTAR EN SELECT PARA TABLA %s", nombre_tabla);
	unlock_semaforo_compactar(nombre_tabla); //fixme sem particion
	log_warning(logger, "SE DESBLOQUEA MUTEX COMPACTAR EN SELECT PARA TABLA %s", nombre_tabla);

	log_warning(logger, "SE INTENTA BLOQUEAR MUTEX MEMTABLE EN SELECT PARA TABLA %s", nombre_tabla);
	pthread_mutex_lock(&mutex_memtable);
	log_warning(logger, "SE BLOQUEA MUTEX MEMTABLE EN SELECT PARA TABLA %s", nombre_tabla);

	//buscar key en memoria temporal
	Tabla* tabla_en_memtable = buscar_tabla(nombre_tabla);

	Registro* registro_max_memtable = malloc(sizeof(Registro));
	registro_max_memtable->timestamp = 0;
	for (i = 0; i < tabla_en_memtable->registros->elements_count; i++) {
		registro_de_memtable = list_get(tabla_en_memtable->registros, i);
		if (key == registro_de_memtable->key) {
			encontrada_en_memtable = 1;
			if (registro_de_memtable->timestamp > registro_max_memtable->timestamp) {
				registro_max_memtable->key = registro_de_memtable->key;
				registro_max_memtable->timestamp = registro_de_memtable->timestamp;
				registro_max_memtable->value = registro_de_memtable->value;
			}
		}
	}
	log_warning(logger, "SE INTENTA DESBLOQUEAR MUTEX MEMTABLE EN SELECT PARA TABLA %s", nombre_tabla);
	pthread_mutex_unlock(&mutex_memtable);
	log_warning(logger, "SE DESBLOQUEA MUTEX MEMTABLE EN SELECT PARA TABLA %s", nombre_tabla);

	//buscar key en archivos temporales de la tabla
	int j = 1;
	char* path_archivo_temporal = armar_path_de_archivo_temporal(j, path_tabla);

	//			GUARDO LOS REGISTROS DE TODOS LOS BLOQUES DEL ARCHIVO EN LA LISTA

	log_warning(logger, "SE INTENTA BLOQUEAR MUTEX COMPACTAR EN SELECT PARAV2 TABLA %s", nombre_tabla);
	lock_semaforo_compactar(nombre_tabla);
	log_warning(logger, "SE BLOQUEAR MUTEX COMPACTAR EN SELECT PARA V2 TABLA %s", nombre_tabla);
	log_warning(logger, "SE INTENTA BLOQUEAR MUTEX PARTICION EN SELECT V2 PARA TABLA %s", nombre_tabla);
	lock_semaforo_particion(nombre_tabla);
	log_warning(logger, "SE BLOQUEA MUTEX PARTICIONE SELECT V2 PARA TABLA %s", nombre_tabla);

	lista_registros_de_archivos_temporales = list_create();

	while (existe_archivo(path_archivo_temporal)) {

		guardar_en_lista_registros_de_archivo(path_archivo_temporal, lista_registros_de_archivos_temporales, cant_particiones);
		free(path_archivo_temporal);
		j++;
		path_archivo_temporal = armar_path_de_archivo_temporal(j, path_tabla);
	}
	free(path_archivo_temporal);
	//Buscamos en la lista de registros el key deseado

	Registro* registro_max_temporal = malloc(sizeof(Registro));
	registro_max_temporal->timestamp = 0;
	for (i = 0; i < lista_registros_de_archivos_temporales->elements_count; i++) {
		registro_de_temporal = list_get(lista_registros_de_archivos_temporales, i);
		if (key == registro_de_temporal->key) {
			encontrada_en_temporal = 1;
			if (registro_de_temporal->timestamp > registro_max_temporal->timestamp) {
				registro_max_temporal->key = registro_de_temporal->key;
				registro_max_temporal->timestamp = registro_de_temporal->timestamp;
				registro_max_temporal->value = registro_de_temporal->value;
			}
		}
	}

	log_warning(logger, "SE INTENTA DESBLOQUEAR MUTEX PARTICION EN SELECT V2 PARA TABLA %s", nombre_tabla);
	unlock_semaforo_particion(nombre_tabla);
	log_warning(logger, "SE DESBLOQUEA MUTEX PARTICION EN SELECT V2 PARA TABLA %s", nombre_tabla);
	log_warning(logger, "SE INTENTA DESBLOQUEAR MUTEX COMPACTAR EN SELECT V2 PARA TABLA %s", nombre_tabla);
	unlock_semaforo_compactar(nombre_tabla);
	log_warning(logger, "SE DESBLOQUEA MUTEX COMPACTAR EN SELECT V2 PARA TABLA %s", nombre_tabla);

	//devolver valor con timestamp mas grande

	if (!encontrada_en_particion && !encontrada_en_memtable && !encontrada_en_temporal) {
		log_error(logger, "La key %d no se encuentra en el LFS", key);
		resultado = crear_paquete(ERROR, "La key no se encuentra en el LFS");
		list_destroy_and_destroy_elements(lista_registros_de_archivos_temporales, (void*) eliminar_registro_compactacion);
		free(path_tabla);
		free(nombre_tabla);
		free(registro_max_memtable);
		free(registro_max_temporal);
		return resultado;
	}

	Registro* registro_maximo_timestamp = malloc(sizeof(Registro));
	registro_maximo_timestamp->timestamp = 0;

	if (encontrada_en_particion) {
		registro_maximo_timestamp->timestamp = registro_de_particion->timestamp;
		registro_maximo_timestamp->key = registro_de_particion->key;
		registro_maximo_timestamp->value = registro_de_particion->value;

	}
	if (encontrada_en_memtable) {
		if (registro_max_memtable->timestamp > registro_maximo_timestamp->timestamp) {
			registro_maximo_timestamp->timestamp = registro_max_memtable->timestamp;
			registro_maximo_timestamp->key = registro_max_memtable->key;
			registro_maximo_timestamp->value = registro_max_memtable->value;
		}
	}

	if (encontrada_en_temporal) {
		if (registro_max_temporal->timestamp > registro_maximo_timestamp->timestamp) {
			registro_maximo_timestamp->timestamp = registro_max_temporal->timestamp;
			registro_maximo_timestamp->key = registro_max_temporal->key;
			registro_maximo_timestamp->value = registro_max_temporal->value;
		}
	}
	log_info(logger, "El numero de key %d mas reciente encontrada es con el timestamp %d y su valor es %s", registro_maximo_timestamp->key, registro_maximo_timestamp->timestamp, registro_maximo_timestamp->value);

	//Armar respuesta para el solicitiante
	char* char_timestamp = string_itoa(registro_maximo_timestamp->timestamp);
	char* char_key = string_itoa(registro_maximo_timestamp->key);

	char* respuesta = string_new();
	string_append(&respuesta, nombre_tabla);
	string_append(&respuesta, " ");
	string_append(&respuesta, char_timestamp);
	string_append(&respuesta, " ");
	string_append(&respuesta, char_key);
	string_append(&respuesta, " ");
	string_append(&respuesta, registro_maximo_timestamp->value);

	resultado = crear_paquete(SELECT, respuesta);
	free(char_timestamp);
	free(char_key);
	free(respuesta);

	//Liberamos memoria AGREGO VALIDACION PORQUE EN
	if (esta_en_particion) {
		list_destroy_and_destroy_elements(registros_de_particion, (void*) eliminar_registro_compactacion);
	}

	list_destroy_and_destroy_elements(lista_registros_de_archivos_temporales, (void*) eliminar_registro_compactacion);

	free(nombre_tabla);
	free(registro_maximo_timestamp);
	free(registro_max_memtable);
	free(registro_max_temporal);
	free(path_tabla);

	return resultado;
}

char* aniadir_bloque_asignado(char* path_particion_o_temporal) {
	if (!cantidad_de_bloques_vacios()) {
		log_warning(logger, "No hay bloques vacios para asignar");
		return (char*) -1;
	}

	int numero_de_primer_bloque_no_ocupado = devolver_numero_de_primer_bloque_no_ocupado();
	bloque_glob = numero_de_primer_bloque_no_ocupado;

	char* char_numero_de_primer_bloque_no_ocupado = string_itoa(numero_de_primer_bloque_no_ocupado);
	t_config* config_archivo = config_create(path_particion_o_temporal);
	char** array_de_bloques_asociados = config_get_array_value(config_archivo, "BLOQUES");
	char* char_array_de_bloques = string_new();

	//Creo cadena de bloques para grabar al archivo
	string_append(&char_array_de_bloques, "[");
	int j = 0;

	while (array_de_bloques_asociados[j] != NULL) {
		string_append(&char_array_de_bloques, array_de_bloques_asociados[j]);
		string_append(&char_array_de_bloques, ",");
		free(array_de_bloques_asociados[j]);
		j++;
	}

	bitarray_set_bit(bitarray, numero_de_primer_bloque_no_ocupado);
	string_append(&char_array_de_bloques, char_numero_de_primer_bloque_no_ocupado);
	string_append(&char_array_de_bloques, "]");
	free(char_numero_de_primer_bloque_no_ocupado);

	config_set_value(config_archivo, "BLOQUES", char_array_de_bloques);
	config_save(config_archivo);

	//fixme save in file ??

	char* path_bloque = seleccionar_bloque_a_manipular(numero_de_primer_bloque_no_ocupado);

	free(array_de_bloques_asociados);
	free(char_array_de_bloques);
	config_destroy(config_archivo);
	return path_bloque;
}

void* manejar_requests() {
	int socket_oyente = escuchar(configuracion_lfs->ip, configuracion_lfs->puerto);

	crear_hilo_por_cada_request(socket_oyente, LFS);

	return NULL;
}

t_list* agregar_registro_repetido_a_lista(t_list* lista_registros_de_particion, Registro_compactacion* registro_compactacion) {
	int i;
	Registro_compactacion* registro_de_bloque;
	for (i = 0; i < list_size(lista_registros_de_particion); i++) {
		registro_de_bloque = list_get(lista_registros_de_particion, i);
		if ((registro_de_bloque->key == registro_compactacion->key) && (registro_de_bloque->timestamp < registro_compactacion->timestamp)) {
			registro_de_bloque->timestamp = registro_compactacion->timestamp;
			registro_de_bloque->value = registro_compactacion->value;
			break;
		}

	}
	if (i == list_size(lista_registros_de_particion)) {
		list_replace_and_destroy_element(lista_registros_de_particion, i, registro_de_bloque, (void*) eliminar_registro_compactacion);
	}

	return lista_registros_de_particion;
}

t_list* agregar_registro_no_repetido_a_lista(t_list* lista_registros_de_particion, Registro_compactacion* registro_compactacion) {
	list_add(lista_registros_de_particion, registro_compactacion);
	return lista_registros_de_particion;
}

void eliminar_registro_compactacion(Registro_compactacion* registro) {
	free(registro->value);
	free(registro);
}

void escribir_lista_registros_en_particion(t_list* lista_registros, char* path_particion) {
	//OBTENGO LOS BLOQUES DE LA PARTICION
	t_config* datos_de_archivo_temporal_listo_compactar = config_create(path_particion);
	char** numeros_bloques_asociados = config_get_array_value(datos_de_archivo_temporal_listo_compactar, "BLOQUES");
	config_destroy(datos_de_archivo_temporal_listo_compactar);
	//Vaciamos los bloques
	int numero_bloque;
	int j = 0;
//	log_error(logger, "Veo numeros de bloques asociados a particoin");
	while (numeros_bloques_asociados[j] != NULL) {
		sscanf(numeros_bloques_asociados[j], "%d", &numero_bloque); //paso el valor del array a int para poder usarlo en la funcion
		char* path_bloque_a_escribir = seleccionar_bloque_a_manipular(numero_bloque);
		vaciar_bloque(path_bloque_a_escribir);
		free(path_bloque_a_escribir);
		j++;
	}

	Registro_compactacion* registro;
	FILE* archivo_bloque_a_escribir;
	char* linea_registro;
	int tamanio_disponible_restante_en_bloque;
	int k = 0; //lista de bloques
	int i = 0;
	sscanf(numeros_bloques_asociados[k], "%d", &numero_bloque); //paso el valor del array a int para poder usarlo en la funcion
	char* path_bloque_a_escribir = seleccionar_bloque_a_manipular(numero_bloque);
	tamanio_disponible_restante_en_bloque = tamanio_bloque_LFS;

	//-------------
	char* char_array_de_bloques = string_new();
	string_append(&char_array_de_bloques, "[");
	string_append(&char_array_de_bloques, numeros_bloques_asociados[k]);

//	log_error(logger, "El numero de bloqus asociados antes del for es %s", char_array_de_bloques);

	//----------------

//	log_error(logger,"Veo numeros de bloques asociados a particoin, voy a iterar %d veces",lista_registros->elements_count);
	for (i = 0; i < lista_registros->elements_count; i++) {	//while?
//		log_error(logger, "VOy por la iteracion %d ", i);
		registro = list_get(lista_registros, i);
		linea_registro = armar_linea_registro_compactacion(registro);
//		log_error(logger, "Esta es la linea de registros: %s", linea_registro);
//		log_error(logger,"Vemos si el tamanio del registro es menor al tamanio disponible");
		if ((strlen(linea_registro) + 1) <= tamanio_disponible_restante_en_bloque) {
//			log_error(logger, "Tamanio registro %d es menor (tiene + 1)",strlen(linea_registro) + 1);
			tamanio_disponible_restante_en_bloque = tamanio_disponible_restante_en_bloque - strlen(linea_registro) - 1;
//			log_error(logger, "Tamanio disponible luego de restar: %d",tamanio_disponible_restante_en_bloque);
			archivo_bloque_a_escribir = txt_open_for_append(path_bloque_a_escribir);
			txt_write_in_file(archivo_bloque_a_escribir, linea_registro);
			txt_write_in_file(archivo_bloque_a_escribir, "\n");
			txt_close_file(archivo_bloque_a_escribir); //Agregado
			//
		} else {
//			log_error(logger, "Tamanio registro %d es mayor (tiene + 1)",strlen(linea_registro) + 1);
			string_append(&linea_registro, "\n");
			char* primera_parte_registro_cortado = string_substring_until(linea_registro, tamanio_disponible_restante_en_bloque);

			archivo_bloque_a_escribir = txt_open_for_append(path_bloque_a_escribir);
			txt_write_in_file(archivo_bloque_a_escribir, primera_parte_registro_cortado);
			txt_close_file(archivo_bloque_a_escribir);
			free(path_bloque_a_escribir);

			if (numeros_bloques_asociados[k + 1] != NULL) {
				k++;

				sscanf(numeros_bloques_asociados[k], "%d", &numero_bloque); //paso el valor del array a int para poder usarlo en la funcion
				//----------------
				string_append(&char_array_de_bloques, ",");
				string_append(&char_array_de_bloques, numeros_bloques_asociados[k]);
//				log_error(logger,"El numero de bloqus asociados dsp del for y en if valido es %s",char_array_de_bloques);
				//--------------------
				path_bloque_a_escribir = seleccionar_bloque_a_manipular(numero_bloque);
			} else {

				path_bloque_a_escribir = aniadir_bloque_asignado(path_particion);
				char* char_bloque_glob = string_itoa(bloque_glob);
				string_append(&char_array_de_bloques, ",");
				string_append(&char_array_de_bloques, char_bloque_glob);
//				log_error(logger,"El numero de bloqus asociados dsp del for y en else es %s",char_array_de_bloques);
				free(char_bloque_glob);
			}

			char* segunda_parte_registro_cortado = string_substring_from(linea_registro, tamanio_disponible_restante_en_bloque);
			tamanio_disponible_restante_en_bloque = tamanio_bloque_LFS - strlen(segunda_parte_registro_cortado);
			archivo_bloque_a_escribir = txt_open_for_append(path_bloque_a_escribir);
			txt_write_in_file(archivo_bloque_a_escribir, segunda_parte_registro_cortado);
			txt_close_file(archivo_bloque_a_escribir);

			free(segunda_parte_registro_cortado);
			free(primera_parte_registro_cortado);
		}
		free(linea_registro);
	}

//	log_error(logger, "El numero de bloqus asociados fuera del for es %s",char_array_de_bloques);
	string_append(&char_array_de_bloques, "]");
//	log_error(logger,"El numero de bloqus asociados luego de apendearel ultimo corchete es %s",	char_array_de_bloques);
	free(path_bloque_a_escribir);
	//grabo en config el bloque array
	t_config* config_particion = config_create(path_particion);

	config_set_value(config_particion, "BLOQUES", char_array_de_bloques);
	config_save(config_particion);
//	char* valorpete = config_get_string_value(config_particion, "BLOQUES");
//	log_error(logger, "el valor grabado es %s", valorpete);

	config_destroy(config_particion);
	free(char_array_de_bloques);

	//----------

	if (k != (j - 1)) {
		k++;
		while (numeros_bloques_asociados[k] != NULL) { //Si lo compactado ocupa menos que lo de antes (puede ser que un registro tenga value mas corto y me termine ocupando menos bloques)
			sscanf(numeros_bloques_asociados[k], "%d", &numero_bloque);
			bitarray_clean_bit(bitarray, numero_bloque);
			k++;
		}
	}
	k = 0;
	while (numeros_bloques_asociados[k] != NULL) {
		free(numeros_bloques_asociados[k]);
		k++;
	}
	free(numeros_bloques_asociados);
	return;
}

void escribir_lista_registros_en_bloque(t_list* lista, char* path_bloque, char* path_particion) {
	int tamanio_anterior = tamanio_bloque_LFS - espacio_disponible_en_bloque(path_bloque);
	vaciar_bloque(path_bloque);

	Registro_compactacion* registro;
	char* linea_registro;
	int tamanio_archivo_bloque = 0;
	int tamanio_auxiliar = 0;

//	int es_bloque_inicial = 1; NO LA USO MAS
	int i;
	FILE* archivo_bloque_a_escribir = txt_open_for_append(path_bloque);
	for (i = 0; i < list_size(lista); i++) {
		registro = list_get(lista, i);
		linea_registro = armar_linea_registro_compactacion(registro);
		tamanio_archivo_bloque += strlen(linea_registro);
		tamanio_auxiliar += strlen(linea_registro);

		// Agregada contingencia de tamanio
		if (tamanio_auxiliar > tamanio_bloque_LFS) {
			tamanio_auxiliar = 0;
			tamanio_auxiliar += strlen(linea_registro);
//			es_bloque_inicial = 1; NO LA USO MAS
			txt_close_file(archivo_bloque_a_escribir);
			free(path_bloque);
			path_bloque = aniadir_bloque_asignado(path_particion);
			archivo_bloque_a_escribir = txt_open_for_append(path_bloque);

		}

//		es_bloque_inicial = 0;   NO LA USO MAS
		txt_write_in_file(archivo_bloque_a_escribir, linea_registro);
		txt_write_in_file(archivo_bloque_a_escribir, "\n");

		free(linea_registro);
	}

	txt_close_file(archivo_bloque_a_escribir);
	t_config* config_particion = config_create(path_particion);
	int tamanio_particion = config_get_int_value(config_particion, "SIZE");
	tamanio_particion = tamanio_particion - tamanio_anterior + tamanio_archivo_bloque;
	char* char_tamanio_particion = string_itoa(tamanio_particion);
	config_set_value(config_particion, "SIZE", char_tamanio_particion);
	config_save(config_particion);
	config_destroy(config_particion);
	free(char_tamanio_particion);

}

int espacio_disponible_en_bloque(char* path_bloque) {
	int espacio_disponible = 0;
	int espacio_ocupado = 0;
//	int saltos_de_linea = 0;
	char* stream_registros_bloque = leer_datos_de_bloque(path_bloque);
	espacio_ocupado = strlen(stream_registros_bloque);

	//SIGNIFICA QUE ARCHIVO ESTA VACIO
	espacio_disponible = tamanio_bloque_LFS - espacio_ocupado;

	//Liberando stream

	free(stream_registros_bloque);
	return espacio_disponible;
}

char* bloque_con_espacio(char* path_particion, char* linea) {
	t_config* datos_de_particion = config_create(path_particion);
	char** numeros_bloques_asociados = config_get_array_value(datos_de_particion, "BLOQUES");
	config_destroy(datos_de_particion);
	int numero_de_bloque;
	int j = 0;
	int espacio_en_bloque = 0;
	char* bloque_con_espacio;

	while (numeros_bloques_asociados[j] != NULL) {
		sscanf(numeros_bloques_asociados[j], "%d", &numero_de_bloque); //paso el valor del array a int para poder usarlo en la funcion
		char* path_bloque = seleccionar_bloque_a_manipular(numero_de_bloque);
		espacio_en_bloque = espacio_disponible_en_bloque(path_bloque);

		free(path_bloque);
		if (espacio_en_bloque > 0) {
			break;
		}
		j++;
	}
	//Si numero_bloques_asociados == NULL (no hay bloques con espacio) creo uno nuevo
	if (numeros_bloques_asociados[j] == NULL) {
		bloque_con_espacio = aniadir_bloque_asignado(path_particion);
	} else {
		bloque_con_espacio = seleccionar_bloque_a_manipular(numero_de_bloque);
	}

	//Libero memoria;
	j = 0;
	while (numeros_bloques_asociados[j] != NULL) {
		free(numeros_bloques_asociados[j]);
		j++;
	}
	free(numeros_bloques_asociados);
	//REVISAR SALIDA
	if (bloque_con_espacio == (char*) -1) {
		return "no hay bloques";
	}

	return bloque_con_espacio;
}

int cantidad_lineas(char* path) { //SE LA ROBE A NICO PERO NO LA USE JEJE
	FILE* fp;
	fp = fopen(path, "r");
	int cantidad_lineas = 0;
	char caracter, caracter_anterior = '\n';
	if (fp == NULL) {
		log_error(logger, "Error al abrir el archivo");
	}
	while ((caracter = fgetc(fp)) != EOF) {
		if (caracter_anterior == '\n') {
			cantidad_lineas++;
		}
		caracter_anterior = caracter;
	}
	fclose(fp);
	return cantidad_lineas;
}

int tamanio_que_ocupara_dump_tabla(Tabla* tabla_en_memtable) {
	int i;

	int tamanio_linea;
	int tamanio_que_ocupara_dump_tabla = 0;
	Registro* registro;
	char* linea_registro;

	for (i = 0; i < tabla_en_memtable->registros->elements_count; i++) {
		registro = list_get(tabla_en_memtable->registros, i);
		linea_registro = armar_linea_registro(registro);
		tamanio_linea = strlen(linea_registro) + 1; //Le sumo 1 por el \n que todavia no se lo agregue
		tamanio_que_ocupara_dump_tabla += tamanio_linea;
		free(linea_registro);
	}

	return tamanio_que_ocupara_dump_tabla;

}

int hay_espacio_para_dumpear_toda_la_tabla(Tabla* tabla_en_memtable) {
	int hay_espacio = 1;
	int cantidad_bloques_vacios = cantidad_de_bloques_vacios();
	int i;

	int tamanio_linea;
	int tamanio_que_ocupara_dump_tabla = 0;
	int tamanio_disponible = cantidad_bloques_vacios * tamanio_bloque_LFS;
	Registro* registro;
	char* linea_registro;

	for (i = 0; i < tabla_en_memtable->registros->elements_count; i++) {
		registro = list_get(tabla_en_memtable->registros, i);
		linea_registro = armar_linea_registro(registro);
		tamanio_linea = strlen(linea_registro) + 1; //Le sumo 1 por el \n que todavia no se lo agregue
		tamanio_que_ocupara_dump_tabla += tamanio_linea;
		free(linea_registro);
	}

	if (tamanio_que_ocupara_dump_tabla > tamanio_disponible) {
		hay_espacio = 0;
	}

	return hay_espacio;
}

int existe_carpeta(char* path_carpeta) {
	DIR* dir = opendir(path_carpeta);

	if (!dir)
		return 0;  //carpeta no existe

	closedir(dir);
	return 1;      //carpeta existe

}

TIPO_CONSISTENCIA convertir_consistencia(char* aux_consistencia) {

	if (string_equals_ignore_case(aux_consistencia, "SC")) {
		return SC;
	} else if (string_equals_ignore_case(aux_consistencia, "SHC")) {
		return SHC;
	} else if (string_equals_ignore_case(aux_consistencia, "EC")) {
		return EC;
	} else
		return ERROR;

}

t_list* crear_lista_sin_registros_repetidos(t_list* lista_registros) {
	t_list* lista_sin_repetidos = list_create();
	Registro* registro_original;
	Registro* registro_sin_rep;
	int flag_repetido;
	int i = 0;
	int j;
	registro_original = list_get(lista_registros, i);
	list_add(lista_sin_repetidos, registro_original);

	for (i = 1; i < lista_registros->elements_count; i++) {
		registro_original = list_get(lista_registros, i);
		flag_repetido = 0;

		for (j = 0; j < lista_sin_repetidos->elements_count; j++) {
			registro_sin_rep = list_get(lista_sin_repetidos, j);
			if (registro_original->key == registro_sin_rep->key) {
				//Si esta repetido
				flag_repetido = 1;
				break;
			} else {
				//Si no esta repetido
			}
		}
		if (flag_repetido) {
			if (registro_original->timestamp > registro_sin_rep->timestamp) {
				//el
				list_remove(lista_sin_repetidos, j);
				list_add(lista_sin_repetidos, registro_original);
			} else {

				//Si el registro sin repetidos es mayor no hacer nada
			}

		} else {
			list_add(lista_sin_repetidos, registro_original);
		}
	}
	return lista_sin_repetidos;
}

