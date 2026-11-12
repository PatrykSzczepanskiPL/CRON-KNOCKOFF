//
// Created by root on 11/10/26.
//

#ifndef MY_CRON_MY_CRON_H
#define MY_CRON_MY_CRON_H

#ifndef CRON_H
#define CRON_H

#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <signal.h>
#include <mqueue.h>
#include <pthread.h>
#include <sys/types.h>
#include <errno.h>
#include <unistd.h>
#include <stdatomic.h>
#include <string.h>
#include <spawn.h>
#include <wait.h>
#include "./logger/logger.h"


// Ścieżki IPC i plików systemowych
#define CRON_LOCK_FILE      "/tmp/my_cron.lock"
#define CRON_SERVER_QUEUE   "/my_cron_server_queue"
#define CRON_CLIENT_QUEUE_FMT "/my_cron_client_%d_queue"
#define SERVER_LOG_FILE "/tmp/cron_logs.log"
#define APP_NAME "CRON"

#define MAX_CMD_LEN         256
#define MAX_ARGS_LEN        256
#define MAX_TASKS           64
#define MSG_SIZE            sizeof(cron_msg_t)

#define SIG_STOP_SERVER (SIGRTMIN+3)


// 1. TYPY I ENUMERACJE

// Typ planowania zadania
typedef enum {
    SCHEDULE_RELATIVE = 0,  // Uruchomienie za X sekund
    SCHEDULE_ABSOLUTE,      // Uruchomienie o konkretnej godzinie (timestamp)
    SCHEDULE_PERIODIC       // Uruchomienie cykliczne co X sekund
} schedule_type_t;

// Typ operacji (polecenia IPC od Klienta do Serwera)
typedef enum {
    CMD_ADD_TASK = 1,       // Dodanie nowego zadania
    CMD_LIST_TASKS,         // Pobranie listy zaplanowanych zadań
    CMD_CANCEL_TASK,        // Anulowanie wskazanego zadania po ID
    CMD_STOP_SERVER,        // Zakończenie pracy serwera
    CMD_RESPONSE            // Odpowiedź serwera do klienta
} command_type_t;

// Status odpowiedzi serwera
typedef enum {
    RES_OK = 0,
    RES_PART,
    RES_ERROR
} response_status_t;


// 2. STRUKTURY DANYCH

// Definicja pojedynczego zadania w systemie
typedef struct {
    uint32_t task_id;               // Unikalny identyfikator zadania
    char command[MAX_CMD_LEN];       // Ścieżka do programu/polecenia
    char args[MAX_ARGS_LEN];         // Argumenty polecenia
    schedule_type_t sched_type;      // Typ harmonogramowania
    time_t target_time;              // Czas uruchomienia (dla ABSOLUTE / RELATIVE)
    uint32_t interval_sec;           // Interwał w sekundach (dla PERIODIC)
    timer_t posix_timer_id;          // Identyfikator zegara POSIX (timer_create)
    bool is_active;                  // Czy zadanie jest aktywne
    struct cron_server_state_t * server_state;
} cron_task_t;

// Struktura komunikatu IPC (przesyłana przez POSIX Message Queue)
typedef struct {
    command_type_t cmd_type;         // Typ polecenia
    pid_t client_pid;                // PID klienta (do wysłania odpowiedzi)

    // Dane dla CMD_ADD_TASK
    cron_task_t task_data;

    // Dane dla CMD_CANCEL_TASK
    uint32_t target_task_id;

    // Dane dla CMD_RESPONSE (odpowiedź od serwera)
    response_status_t status;
    char response_payload[1024];     // Tekstowa odpowiedź (np. lista zadań lub błąd)
} cron_msg_t;

// Główna struktura stanu Serwera
typedef struct {
    int lock_fd;                     // Deskryptor pliku blokady single-instance
    mqd_t server_mq;                 // Deskryptor kolejki wejściowej serwera
    cron_task_t tasks[MAX_TASKS];    // Tablica zaplanowanych zadań
    uint32_t next_task_id;           // Licznik do generowania ID zadań
    pthread_mutex_t tasks_mutex;     // Mutex chroniący tablicę zadań
    atomic_bool is_running;        // Flaga pracy serwera
    pthread_t signal_thread;
} cron_server_state_t;


// 3. PROTOTYPY FUNKCJI SYSTEMOWYCH I MODUŁÓW

/*
 * DETEKCJA INSTANCJI (Single-Instance)
 * Zwraca: 1 jeśli proces jest SERWEREM (pierwsza instancja),
 *         0 jeśli jest KLIENTEM (serwer już działa),
 *        -1 w przypadku błędu krytycznego.
 */
int is_first_instance(int *lock_fd);

/*
 * MODUŁ SERWERA
 */
// Inicjalizacja i pętla główna serwera
int server_run(int lock_fd);
int server_init(cron_server_state_t *state);


// Sprzątanie zasobów serwera przy zamknięciu (wyrejestrowanie timerów, kolejki, logera)
void server_cleanup(cron_server_state_t *state);
void * server_signal_thread(void * args);
/*
 * MODUŁ ZARZĄDZANIA ZADANIAMI I ZEGARAMI (POSIX Timers)
 */

void copy_task(cron_task_t * target, cron_task_t * source);
// Dodaje zadanie do struktury i uzbraja zegar POSIX (timer_create + timer_settime)
// zwraca 0 jeżeli sukces -1 jeżeli brak miejsca na nowy task
int task_manager_add(cron_server_state_t *state, cron_task_t *new_task);

// Usuwa zadanie o podanym ID i rozbraja/niszczy odpowiadający mu zegar POSIX
int task_manager_cancel(cron_server_state_t *state, uint32_t task_id);

// Generuje sformatowaną listę zaplanowanych zadań do bufora wyjściowego
int task_manager_list(cron_server_state_t *state, cron_msg_t * res);

// Anuluje wszystkie zadania i niszczy ich zegary (wywoływane przy zamknięciu)
int task_manager_cancel_all(cron_server_state_t *state);

// Callback / Handler wywoływany przez Zegar POSIX w momencie upłynięcia czasu zadania
void task_execution_handler(union sigval sv);


/*
 * MODUŁ KLIENTA
 */
// Przetwarza argumenty CLI i wysyła odpowiednie zlecenie IPC do serwera

static void print_help(const char *prog_name);

int client_run(int argc, char *argv[]);

// Parsowanie czasu podanego z CLI (np. względny +10s, bezwzględny HH:MM, cykliczny *5s)
int parse_schedule_args(const char *time_str, schedule_type_t *type, time_t *target_time, uint32_t *interval);


/*
 * MODUŁ KOMUNIKACJI IPC (POSIX Message Queues)
 */

// Wysyła komunikat z Klienta do Serwera i czeka na odpowiedź w dedykowanej kolejce zwrotnej
int ipc_client_send_and_receive(cron_msg_t *req, cron_msg_t *res);

// Wysłanie odpowiedzi z Serwera do konkretnego Klienta
int ipc_server_send_response(cron_msg_t *res);

#endif // CRON_H


#endif //MY_CRON_MY_CRON_H
