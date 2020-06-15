#include "sockets.h"

//-----------------------------------------------------------------Funciones auxiliares----------------------------------------------------------------

int recibir_instruccion(int socket) {
	int instruccion;
	//log_error(logger, "Recibo instruccion");//todo volar
	int prueba = recv(socket, &instruccion, sizeof(int), MSG_WAITALL);
	//log_error(logger, "%d", prueba );//todo volar
	if (prueba) {
		//	log_error(logger, "Devuelvo instruccion");//todo volar
		return instruccion;
	} else {
		//	log_error(logger, "Devuelvo -1 y cierro el socket");//todo volar
		close(socket);
		return -1;
	}
}

Buffer* recibir_buffer(int socket_cliente) {

	int tamanio;
	Buffer* buffer = malloc(sizeof(Buffer));

	recv(socket_cliente, &tamanio, sizeof(int), MSG_WAITALL);
	//log_error(logger, "Recibo tamanio");//todo volar
	buffer->tamanio = tamanio;
	buffer->stream = malloc(buffer->tamanio);
	recv(socket_cliente, buffer->stream, tamanio, MSG_WAITALL);
	//log_error(logger, "Recibo stream");//todo volar

	return buffer;

}

void* serializar(Paquete* paquete, int bytes) {

	void* datos = malloc(bytes);
	int desplazamiento = 0;

	memcpy(datos + desplazamiento, &(paquete->instruccion), sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(datos + desplazamiento, &(paquete->buffer->tamanio), sizeof(int));
	desplazamiento += sizeof(int);
	memcpy(datos + desplazamiento, paquete->buffer->stream, paquete->buffer->tamanio);
	desplazamiento += paquete->buffer->tamanio;

	return datos;
}

Operacion obtener_instruccion(char* instruccion_leida) {
	Operacion instruccion;

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
	} else {
		log_warning(logger, "Entrada invalida!");
	}

	return instruccion;
}

//-----------------------------------------------------------------Funciones principales--------------------------------------------------------------

void eliminar_paquete(Paquete* paquete) {
	free(paquete->buffer->stream);
	free(paquete->buffer);
	free(paquete);
}

Paquete* recibir(int socket) {

	Paquete* paquete = malloc(sizeof(Paquete));
	paquete->instruccion = recibir_instruccion(socket);
	paquete->buffer = recibir_buffer(socket);

	return paquete;
}

int conectar(char* ip, char* puerto, Modulo quien_soy) {
	struct addrinfo hints;
	struct addrinfo *server_info;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_flags = AI_PASSIVE;

	getaddrinfo(ip, puerto, &hints, &server_info);

	int socket_servidor = socket(server_info->ai_family, server_info->ai_socktype, server_info->ai_protocol);

	if (connect(socket_servidor, server_info->ai_addr, server_info->ai_addrlen) == -1) {
		perror("Error al intentar conectarse al servidor");
		freeaddrinfo(server_info);
		return -1;
	}

	freeaddrinfo(server_info);

	enviar_saludo(socket_servidor, quien_soy);
	if (recibir_saludo(socket_servidor, quien_soy) == -1) {
		log_info(logger, "No se recibio el saludo correcto");
		socket_servidor = -1;
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
		if ((socket_oyente = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
			continue;

		int activado = 1;
		setsockopt(socket_oyente, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado));

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

void enviar(int socket, char* mensaje) {
	char str[strlen(mensaje)];
	strcpy(str, mensaje);
	char* token;
	char* resto = str;

	token = strtok_r(resto, " ", &resto);

	//log_warning(logger, "Se obtiene la instrucción"); //todo volar

	Operacion instruccion = obtener_instruccion(token);

	//log_warning(logger, "Se obtuvo la instrucción y se crea el paquete"); //todo volar

	Paquete* paquete = crear_paquete(instruccion, resto);

	//log_warning(logger, "Se creo el paquete %s y se envia", paquete->buffer->stream); //todo volar

	enviar_paquete(socket, paquete);

	//log_warning(logger, "Se envió el paquete"); //todo volar
}

Paquete* crear_paquete(Operacion instruccion, char* mensaje) {
	Paquete* paquete = malloc(sizeof(Paquete));
	paquete->instruccion = instruccion;
	paquete->buffer = malloc(sizeof(Buffer));
	paquete->buffer->tamanio = strlen(mensaje) + 1;
	paquete->buffer->stream = malloc(paquete->buffer->tamanio);
	memcpy(paquete->buffer->stream, mensaje, paquete->buffer->tamanio);

	return paquete;
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

	//log_error(logger, "Bytes enviados: %d", bytes_a_enviar); //todo volar
	//log_error(logger, "Bytes enviados: %d", bytes_enviados); //todo volar

	eliminar_paquete(paquete);
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
		if (cliente != POOL) {
			return -1;
		}
		break;
	}

	return 0;
}

Paquete* recibir_paquete(int socket) {

	Paquete* paquete = malloc(sizeof(Paquete));
	paquete->instruccion = recibir_instruccion(socket);
	paquete->buffer = recibir_buffer(socket);

	return paquete;
}
