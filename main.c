#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include "my_cron.h"

int main(int argc, char *argv[]) {
    int lock_fd = -1;
    int is_server = is_first_instance(&lock_fd);

    if (is_server < 0) {
        return 1;
    }

    if (is_server == 1) {
        if (argc > 1) {
            pid_t pid = fork();
            if (pid == 0) {
                usleep(50000);
                return client_run(argc, argv);
            }
        }

        return server_run(lock_fd);

    } else {
        return client_run(argc, argv);
    }
}