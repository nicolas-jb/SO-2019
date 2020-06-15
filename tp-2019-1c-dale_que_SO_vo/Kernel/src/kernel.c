#include "kernel.h"

int main(void) {

	// Inicialización
	init_logger();
	init_config();
	init_estados_panif();
	init_listas_de_mem_y_criterios();
	cantidad_de_hilos_en_exec = 0;
	id_memoria = 0;
	flag_ejecutar = 1;
	flag_primera_vez = 1;
	flag_primer_describe = 1;
	flag_eliminar_paquete = 1;

	long aux = time(NULL);
	semilla=(aux-150000000);

	SC = malloc(sizeof(S_Consistency));
	EC = malloc(sizeof(S_Consistency));
	SHC = malloc(sizeof(S_Consistency));

	limpiar_consistencias(SC);
	limpiar_consistencias(EC);
	limpiar_consistencias(SHC);

	//Agregar en todas los init de hilos la validación por si da error

	pthread_mutex_init(&mutex_id_script, NULL);
	pthread_mutex_init(&mutex_ingreso_archivo_config, NULL);
	pthread_mutex_init(&mutex_exec, NULL);
	pthread_mutex_init(&mutex_lectura_memorias, NULL);
	pthread_mutex_init(&mutex_metricas, NULL);
	pthread_mutex_init(&mutex_metadata, NULL);
	pthread_mutex_init(&mutex_memorias, NULL);
	pthread_mutex_init(&mutex_carrera, NULL);

	sem_init(&s_espera_conexion_con_pool, 0, 0);
	sem_init(&s_hay_elementos_en_new, 0, 0);
	sem_init(&s_hay_elementos_en_ready, 0, 0);
	sem_init(&s_se_termino_un_exec, 0, 0);
	sem_init(&s_espero_el_primer_describe, 0, 0);

	pthread_create(&hilo_consulta_memorias, NULL, consultar_memorias, NULL);
	pthread_create(&hilo_consola, NULL, leer_consola, NULL);
	// todo sacar comentarios
	pthread_create(&hilo_notif, NULL, notificaciones, NULL);

	/*No importa que aca los hilos sean con el mismo nombre porque no los voy a matar o esperar el join,
	 * van a ejecutar to do el tiempo en simultáneo
	 */

	pthread_create(&hilo_cola_ready, NULL, pasar_a_ready, NULL);
	pthread_create(&hilo_cola_ejex, NULL, ejecutar, NULL);
	pthread_create(&hilo_metricas, NULL, mostrar_las_metricas, NULL);
	pthread_create(&hilo_describe_scheduleado, NULL, describe_scheduleado, NULL);

	/*  Los siguientes join, destroy, terminar, etc.. son protocolares por decirlo de alguna maneera, el programa
	 *  va a estar corriendo to do el tiempo, se agregó en la consola la instrucción de salir para terminar la
	 *  ejecución aunque tampoco se utilizará
	 */
	pthread_join(hilo_consulta_memorias, NULL);
	pthread_join(hilo_metricas, NULL);
	pthread_join(hilo_consola, NULL);
	pthread_join(hilo_notif, NULL);
	pthread_join(hilo_cola_ready, NULL);
	pthread_join(hilo_cola_ejex, NULL);
	pthread_join(hilo_describe_scheduleado, NULL);




	//Protocolo
	terminar_programa();

	return 0;
}

