/*
 * notify.c
 *
 *  Created on: 6 jun. 2019
 *      Author: utnso
 */

#include "notify.h"
#include "kernel.h"

void* notificaciones() {

	char buffer[BUF_LEN];
	int offset = 0;
	int file_descriptor = inotify_init();
	if (file_descriptor < 0) {
		log_error(logger, "error en inotify_init");
	}

	int watch_descriptor = inotify_add_watch(file_descriptor,
			"/home/utnso/Escritorio/tp-2019-1c-dale_que_SO_vo/Kernel",
			IN_MODIFY);

	if (watch_descriptor == -1) {
		log_error(logger, "error en agregar monitor");
	}

	while (1)
	{
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
					pthread_mutex_lock(&mutex_ingreso_archivo_config);
					config_destroy(config);
					cargar_parametros_modificados();
					pthread_mutex_unlock(&mutex_ingreso_archivo_config);
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
