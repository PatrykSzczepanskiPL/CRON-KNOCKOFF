//
// Created by root on 11/4/26.
//

#ifndef SERVER_LOGGER_H
#define SERVER_LOGGER_H
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <stdatomic.h>
#include <unistd.h>
#include <time.h>
#include <pthread.h>
#include <semaphore.h>
#include <string.h>
#include <stdarg.h>

#define SIG_TOGGLE_LOGGING (SIGRTMIN)
#define SIG_CHANGE_LOGGING_SEVERITY (SIGRTMIN +1)
#define SIG_MAKE_DUMP (SIGRTMIN +2)



typedef enum {
    LOG_LEVEL_MIN = 10,
    LOG_LEVEL_STANDARD = 20,
    LOG_LEVEL_MAX = 30,
    LOG_LEVEL_SYSTEM = 100
} log_level_t;

typedef void (*dump_callback_t)(FILE *dump_file);




typedef struct {
    FILE *log_file;
    dump_callback_t app_dump;
    char app_name[64];
    volatile int dump_counter;

    pthread_mutex_t log_mutex;
    pthread_t dump_thread_tid;
    pthread_t toggle_thread_tid;
    pthread_t level_thread_tid;
    atomic_int is_working_flag;

    atomic_int is_logging_enabled;
    atomic_int current_level_pending;
    volatile log_level_t current_level_factual;


    sem_t pending_dump_counter_sem;
    sem_t toggle_sem;
    sem_t change_level_sem;



} logger_state_t;




/*
Inicjalizuje całą biblioteke
otwiera plik loggów
inicjalizuje semafory wątków-sygnałów
ustawia maski sygnałów dla wątków i wątku głównego
uruchamia wątki
*/
int logger_init(const char *log_filename, const char *app_name, dump_callback_t app_dump_func);



/*
Wpisuje do pliku logów podaną wiadomość
 */
int logger_log(log_level_t severity, const char *format, ...);


/*
 Niszczy całą biblioteke
 inkrementruje semafory, żeby wątki sie zniszczyły
 niszczy wątki
 niszczy semafory
 niszczy mute
 zamyka plik logów
*/
void logger_destroy(void);


/*
asynchroniczny handler sygnału dump
*/
void async_handler_dump(int);


/*
asynchroniczny handler synału log
*/
void async_handler_toggle_log(int);


/*
asynchroniczny handler syngału zmianu sygnału logowania
*/
void async_handler_change_level(int signo, siginfo_t *info, void *context);



/*
synchroniczny wątek logowania
 */
void * toggle_log_thread(void * arg);
/*
 synchroniczny wątek zmiany szczegółowości logowania
 */
void* change_log_severity_thread(void *arg);

/*
 Synchroniczny wątek zrzutu aplikacji
 */
void* dump_thread(void *arg);





#endif //SERVER_LOGGER_H
