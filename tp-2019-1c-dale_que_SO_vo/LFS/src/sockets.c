#include "sockets.h"

//-----------------------------------------------------------------Funciones auxiliares----------------------------------------------------------------

int recibir_instruccion(int socket) {
	int instruccion;
	if (recv(socket, &instruccion, sizeof(int), MSG_WAITALL) != 0) {
		return instruccion;
	} else {
		close(socket);
		return -1;
	}
}

Buffer* recibir_buffer(int socket_cliente) {

	int tamanio;
	Buffer* buffer = malloc(sizeof(Buffer));

	recv(socket_cliente, &tamanio, sizeof(int), MSG_WAITALL);
	buffer->tamanio = tamanio;
	buffer->stream = malloc(tamanio);
	recv(socket_cliente, buffer->stream, tamanio, MSG_WAITALL);

	return buffer;

}

Paquete* crear_paquete(Instruccion instruccion, char* mensaje) {
	Paquete* paquete = malloc(sizeof(Paquete));
	paquete->instruccion = instruccion;
	paquete->buffer = malloc(sizeof(Buffer));
	paquete->buffer->tamanio = strlen(mensaje) + 1;
	paquete->buffer->stream = malloc(paquete->buffer->tamanio);
	memcpy(paquete->buffer->stream, mensaje, paquete->buffer->tamanio);

	return paquete;
}

Instruccion obtener_instruccion(char* instruccion_leida) {
	Instruccion instruccion;

	string_to_upper(instruccion_leida); // Agregado porque si es con minuscula genera errores

	if (strcmp(instruccion_leida, "SELECT") == 0) {
		instruccion = SELECT;
	} else if (strcmp(instruccion_leida, "INSERT") == 0) {
		instruccion = INSERT;
	} else if (strcmp(instruccion_leida, "CREATE") == 0) {
		instruccion = CREATE;
	} else if (strcmp(instruccion_leida, "DESCRIBE") == 0) {
		instruccion = DESCRIBE;
	} else if (strcmp(instruccion_leida, "DROP") == 0) {
		instruccion = DROP;
	} else if (strcmp(instruccion_leida, "JOURNAL") == 0) {
		instruccion = JOURNAL;
	} else if (strcmp(instruccion_leida, "ADD") == 0) {
		instruccion = ADD;
	} else if (strcmp(instruccion_leida, "RUN") == 0) {
		instruccion = RUN;
	} else if (strcmp(instruccion_leida, "METRICS") == 0) {
		instruccion = METRICS;
	} else if (strcmp(instruccion_leida, "MEMORIA") == 0) {
		instruccion = MEMORIA;
	} else if (strcmp(instruccion_leida, "TAMANIO_VALUE") == 0) {
		instruccion = TAMANIO_VALUE;
	} else if (strcmp(instruccion_leida, "FINALIZAR") == 0) {
		instruccion = FINALIZAR;
	} else if (strcmp(instruccion_leida, "GOSSIPING") == 0) {
		instruccion = GOSSIPING;
	} else {
		log_warning(logger, "Entrada invalida!");
	}

	return instruccion;
}

void* serializar(Paquete* paquete, int bytes) {

	void* datos = malloc(bytes);
	int desplazamiento = 0;

	memcpy(datos + desplazamiento, &(paquete->instruccion), sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(datos + desplazamiento, &(paquete->buffer->tamanio), sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(datos + desplazamiento, paquete->buffer->stream,
			paquete->buffer->tamanio);
	desplazamiento += paquete->buffer->tamanio;

	return datos;

}

Paquete* deserializar(int socket) {

	Paquete* paquete = malloc(sizeof(Paquete));
	paquete->instruccion = recibir_instruccion(socket);
	paquete->buffer = malloc(sizeof(Buffer));
	paquete->buffer = recibir_buffer(socket);

	return paquete;

}

//-----------------------------------------------------------------Funciones principales--------------------------------------------------------------

void iniciar_logger() {
	logger = log_create("log.log", "Logger", 1, LOG_LEVEL_INFO);

	log_info(logger, "Se ha creado un log.");
}

//Adaptar para cuando el insert es de 4 o de 5 con o sin timestamp

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
	printf("SALIR			- Salir del sistema\n");
	printf("-----------------------------------------------\n");
	printf("-----------------------------------------------\n");
	printf("Ingrese Input\n");
}

int validar_consistencia_consola(char* texto) {
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

int conectar(char* ip, char* puerto, Modulo quien_soy) {
	struct addrinfo hints;
	struct addrinfo *server_info;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	getaddrinfo(ip, puerto, &hints, &server_info);

	int socket_servidor = socket(server_info->ai_family,
			server_info->ai_socktype, server_info->ai_protocol);

	if (connect(socket_servidor, server_info->ai_addr, server_info->ai_addrlen)
			== -1) {
		perror("Error al intentar conectarse al servidor");
		freeaddrinfo(server_info);
		return -1;
	}

	freeaddrinfo(server_info);

	enviar_saludo(socket_servidor, quien_soy);
	if (recibir_saludo(socket_servidor, quien_soy) != -1) {
		log_info(logger, "Conexión al servidor establecida");
	} else {
		socket_servidor = -1;
		log_info(logger, "No se recibio el saludo correcto");
	}

	return socket_servidor;
}

int escuchar(char* ip, char* puerto) {

	int socket_oyente;
	struct addrinfo hints, *servinfo, *p;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	getaddrinfo(ip, puerto, &hints, &servinfo);

	for (p = servinfo; p != NULL; p = p->ai_next) {
		if ((socket_oyente = socket(p->ai_family, p->ai_socktype,
				p->ai_protocol)) == -1)
			continue;

		int activado = 1;
		setsockopt(socket_oyente, SOL_SOCKET, SO_REUSEADDR, &activado,
				sizeof(activado));

		if (bind(socket_oyente, p->ai_addr, p->ai_addrlen) == -1) {
			perror("Fallo el bind.");
			close(socket_oyente);
			continue;
		}
		break;
	}

	listen(socket_oyente, SOMAXCONN);

	freeaddrinfo(servinfo);

	log_info(logger, "Listo para escuchar a mis clientes");

	return socket_oyente;
}

void enviar_mensaje(int socket, char* mensaje) {
	char str[strlen(mensaje)];
	strcpy(str, mensaje);
	char* token;
	char* resto = str;

	token = strtok_r(resto, " ", &resto);

	Instruccion instruccion = obtener_instruccion(token);

	Paquete* paquete = crear_paquete(instruccion, resto);

	enviar_paquete(socket, paquete);
}

void enviar_paquete(int socket, Paquete* paquete) {
	int bytes_a_enviar = paquete->buffer->tamanio + 2 * sizeof(int);
	void* paquete_serializado = serializar(paquete, bytes_a_enviar);

	//Enviar
	int bytes_enviados = send(socket, paquete_serializado, bytes_a_enviar,
	MSG_NOSIGNAL);

	if (bytes_enviados != bytes_a_enviar)
		log_error(logger, "Error al enviar paquete!");

	free(paquete_serializado);
	free(paquete->buffer->stream);
	free(paquete->buffer);
	free(paquete);
}

Paquete* recibir_paquete(int socket) {

	Paquete* paquete = malloc(sizeof(Paquete));
	paquete->instruccion = recibir_instruccion(socket);
	paquete->buffer = recibir_buffer(socket);

	return paquete;
}

void eliminar_paquete(Paquete* paquete) {
	free(paquete->buffer->stream);
	free(paquete->buffer);
	free(paquete);
}

void* atender_request(void* socket_original) {
	int socket_cliente = *(int*) socket_original;

	Paquete* paquete_peticion = recibir_paquete(socket_cliente);
	log_info(logger, "Esperando %d segundos del tiempo de retardo",
			configuracion_lfs->retardo);
	sleep(configuracion_lfs->retardo); //implementacion tiempo de retardo, testear como funciona

	Paquete* paquete_respuesta;

	int instruccion = paquete_peticion->instruccion;

	switch (instruccion) {
	case CREATE:
		log_info(logger, "Peticion de CREATE: %s",
				paquete_peticion->buffer->stream);
		paquete_respuesta = create(paquete_peticion);
		eliminar_paquete(paquete_peticion);
		log_info(logger, "Respuesta de CREATE: %s",
				paquete_respuesta->buffer->stream);
		enviar_paquete(socket_cliente, paquete_respuesta);
		break;
	case INSERT:
		log_info(logger, "Peticion de INSERT: %s",
				paquete_peticion->buffer->stream);

		paquete_respuesta = insert(paquete_peticion);

		eliminar_paquete(paquete_peticion);
		log_info(logger, "Respuesta de INSERT: %s",
				paquete_respuesta->buffer->stream);
		enviar_paquete(socket_cliente, paquete_respuesta);
		break;
	case SELECT:
		log_info(logger, "Peticion de SELECT: %s",
				paquete_peticion->buffer->stream);
//		pthread_mutex_lock(&mutex);
		paquete_respuesta = select_lfs(paquete_peticion);
//		pthread_mutex_unlock(&mutex);
		eliminar_paquete(paquete_peticion);
		log_info(logger, "Respuesta de SELECT: %s",
				paquete_respuesta->buffer->stream);
		enviar_paquete(socket_cliente, paquete_respuesta);
		break;
	case DESCRIBE:
		log_info(logger, "Peticion de DESCRIBE: %s",
				paquete_peticion->buffer->stream);
		paquete_respuesta = describe(paquete_peticion);
		eliminar_paquete(paquete_peticion);
		log_info(logger, "Respuesta de DESCRIBE: %s",
				paquete_respuesta->buffer->stream);
		enviar_paquete(socket_cliente, paquete_respuesta);
		break;
	case DROP:
		log_info(logger, "Peticion de DROP: %s",
				paquete_peticion->buffer->stream);
//		pthread_mutex_lock(&mutex);
		paquete_respuesta = drop(paquete_peticion);
//		pthread_mutex_unlock(&mutex);
		log_info(logger, "Respuesta de DROP: %s",
				paquete_respuesta->buffer->stream);
		eliminar_paquete(paquete_peticion);
		enviar_paquete(socket_cliente, paquete_respuesta);
		break;
	case TAMANIO_VALUE:
		log_info(logger, "Enviando el tamanio maximo del Value");
		char* tamanio_value = string_itoa(configuracion_lfs->tamanio_value);
		paquete_respuesta = crear_paquete(TAMANIO_VALUE, tamanio_value);
		eliminar_paquete(paquete_peticion);
		enviar_paquete(socket_cliente, paquete_respuesta);
		free(tamanio_value);
		break;
	}

	close(socket_cliente);
	free(socket_original);
	return NULL;
}

void enviar_saludo(int socket, Modulo quien_soy) {
	send(socket, &quien_soy, sizeof(Modulo), MSG_NOSIGNAL);
}

int recibir_saludo(int socket, Modulo quien_soy) {
	Modulo cliente;
	recv(socket, &cliente, sizeof(Modulo), MSG_WAITALL);

	switch (quien_soy) {
	case KERNEL:
		if (cliente != POOL) {
			return -1;
		}
		break;
	case POOL:
		if (cliente != KERNEL && cliente != LFS) {
			return -1;
		}
		break;
	case LFS:
		if (cliente != POOL && cliente != LFS) {
			return -1;
		}
		break;
	}

	return 0;
}

