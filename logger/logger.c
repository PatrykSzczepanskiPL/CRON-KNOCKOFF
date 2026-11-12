#include "logger.h"
#include <stdarg.h>

logger_state_t g_loggerState;

int logger_init(const char *log_filename, const char* app_name, dump_callback_t app_dump_func) {
    if (!log_filename || !app_dump_func || !app_name) {
        return -1;
    }

    strncpy(g_loggerState.app_name, app_name, sizeof(g_loggerState.app_name) - 1);
    g_loggerState.app_name[sizeof(g_loggerState.app_name) - 1] = '\0';
    g_loggerState.app_dump = app_dump_func;
    g_loggerState.is_logging_enabled = 1;
    g_loggerState.current_level_pending = LOG_LEVEL_STANDARD;
    g_loggerState.current_level_factual = LOG_LEVEL_STANDARD;
    g_loggerState.dump_counter = 0;
    g_loggerState.is_working_flag = 1;

    g_loggerState.log_file = fopen(log_filename, "a");
    if (g_loggerState.log_file == NULL) {
        return -1;
    }
    if (pthread_mutex_init(&g_loggerState.log_mutex, NULL) != 0) {
        fclose(g_loggerState.log_file);
        return -1;
    }

    int sem_dump_ok = (sem_init(&g_loggerState.pending_dump_counter_sem, 0, 0) == 0);
    int sem_toggle_ok = sem_dump_ok && (sem_init(&g_loggerState.toggle_sem, 0, 0) == 0);
    int sem_level_ok = sem_toggle_ok && (sem_init(&g_loggerState.change_level_sem, 0, 0) == 0);

    if (!sem_dump_ok || !sem_toggle_ok || !sem_level_ok) {
        if (sem_dump_ok)   sem_destroy(&g_loggerState.pending_dump_counter_sem);
        if (sem_toggle_ok) sem_destroy(&g_loggerState.toggle_sem);
        if (sem_level_ok)  sem_destroy(&g_loggerState.change_level_sem);

        pthread_mutex_destroy(&g_loggerState.log_mutex);
        fclose(g_loggerState.log_file);
        return -1;
    }

    sigset_t set;
    sigfillset(&set);
    pthread_sigmask(SIG_SETMASK, &set, NULL);

    struct sigaction act;
    memset(&act, 0, sizeof(act));
    act.sa_mask = set;

    act.sa_flags = SA_SIGINFO;
    act.sa_sigaction = async_handler_change_level;
    sigaction(SIG_CHANGE_LOGGING_SEVERITY, &act, NULL);

    act.sa_flags = 0;
    act.sa_handler = async_handler_dump;
    sigaction(SIG_MAKE_DUMP, &act, NULL);

    act.sa_handler = async_handler_toggle_log;
    sigaction(SIG_TOGGLE_LOGGING, &act, NULL);

    int thread_dump_ok = (pthread_create(&g_loggerState.dump_thread_tid, NULL, dump_thread, NULL) == 0);
    int thread_toggle_ok = (pthread_create(&g_loggerState.toggle_thread_tid, NULL, toggle_log_thread, NULL) == 0);
    int thread_level_ok = (pthread_create(&g_loggerState.level_thread_tid, NULL, change_log_severity_thread, NULL) == 0);

    if (!thread_dump_ok || !thread_toggle_ok || !thread_level_ok) {
        g_loggerState.is_working_flag = 0;
        sem_post(&g_loggerState.pending_dump_counter_sem);
        sem_post(&g_loggerState.change_level_sem);
        sem_post(&g_loggerState.toggle_sem);

        if (thread_level_ok)  pthread_join(g_loggerState.level_thread_tid, NULL);
        if (thread_toggle_ok) pthread_join(g_loggerState.toggle_thread_tid, NULL);
        if (thread_dump_ok)   pthread_join(g_loggerState.dump_thread_tid, NULL);

        sem_destroy(&g_loggerState.pending_dump_counter_sem);
        sem_destroy(&g_loggerState.change_level_sem);
        sem_destroy(&g_loggerState.toggle_sem);
        pthread_mutex_destroy(&g_loggerState.log_mutex);
        fclose(g_loggerState.log_file);
        return -1;
    }

    return 0;
}

int logger_log(log_level_t severity, const char *format, ...) {
    if ((!g_loggerState.is_logging_enabled && severity != LOG_LEVEL_SYSTEM) || severity < g_loggerState.current_level_factual) {
        return 0;
    }

    if (!g_loggerState.log_file || !format) {
        return -1;
    }

    char lvl[4] = {'\0'};
    switch (severity) {
        case LOG_LEVEL_MIN:      sprintf(lvl, "MIN"); break;
        case LOG_LEVEL_STANDARD: sprintf(lvl, "STD"); break;
        case LOG_LEVEL_MAX:      sprintf(lvl, "MAX"); break;
        case LOG_LEVEL_SYSTEM:   sprintf(lvl, "SYS"); break;
    }

    pthread_mutex_lock(&g_loggerState.log_mutex);
    fprintf(g_loggerState.log_file, "[%s] ", lvl);

    va_list args;
    va_start(args, format);
    int written = vfprintf(g_loggerState.log_file, format, args);
    va_end(args);
    fflush(g_loggerState.log_file);
    pthread_mutex_unlock(&g_loggerState.log_mutex);

    return written;
}

void logger_destroy(void) {
    g_loggerState.is_working_flag = 0;

    sem_post(&g_loggerState.pending_dump_counter_sem);
    sem_post(&g_loggerState.toggle_sem);
    sem_post(&g_loggerState.change_level_sem);

    pthread_join(g_loggerState.dump_thread_tid, NULL);
    pthread_join(g_loggerState.toggle_thread_tid, NULL);
    pthread_join(g_loggerState.level_thread_tid, NULL);

    sem_destroy(&g_loggerState.pending_dump_counter_sem);
    sem_destroy(&g_loggerState.toggle_sem);
    sem_destroy(&g_loggerState.change_level_sem);

    pthread_mutex_destroy(&g_loggerState.log_mutex);

    if (g_loggerState.log_file != NULL) {
        fclose(g_loggerState.log_file);
        g_loggerState.log_file = NULL;
    }
}

void async_handler_dump(int sig) {
    (void)sig;
    sem_post(&g_loggerState.pending_dump_counter_sem);
}

void async_handler_toggle_log(int sig) {
    (void)sig;
    sem_post(&g_loggerState.toggle_sem);
}

void async_handler_change_level(int signo, siginfo_t *info, void *context) {
    (void)signo;
    (void)context;
    if (info != NULL) {
        g_loggerState.current_level_pending = info->si_value.sival_int;
    }
    sem_post(&g_loggerState.change_level_sem);
}

void* toggle_log_thread(void *arg) {
    (void)arg;

    sigset_t sigset;
    sigfillset(&sigset);
    sigdelset(&sigset, SIG_TOGGLE_LOGGING);
    pthread_sigmask(SIG_SETMASK, &sigset, NULL);

    while (g_loggerState.is_working_flag) {
        if (sem_wait(&g_loggerState.toggle_sem) == 0) {
            if (!g_loggerState.is_working_flag) break;

            if (g_loggerState.is_logging_enabled) {
                g_loggerState.is_logging_enabled = 0;
                logger_log(LOG_LEVEL_SYSTEM, "Wylaczono logowanie");
            } else {
                g_loggerState.is_logging_enabled = 1;
                logger_log(LOG_LEVEL_SYSTEM, "Wlaczono logowanie");
            }
        }
    }

    return NULL;
}

void* change_log_severity_thread(void *arg) {
    (void)arg;

    sigset_t sigset;
    sigfillset(&sigset);
    sigdelset(&sigset, SIG_CHANGE_LOGGING_SEVERITY);
    pthread_sigmask(SIG_SETMASK, &sigset, NULL);

    char msg[4] = {'\0'};
    while (g_loggerState.is_working_flag) {
        if (sem_wait(&g_loggerState.change_level_sem) == 0) {
            if (!g_loggerState.is_working_flag) break;

            log_level_t current_levele = g_loggerState.current_level_pending;
            switch (current_levele) {
                case LOG_LEVEL_MIN:      sprintf(msg, "MIN"); break;
                case LOG_LEVEL_STANDARD: sprintf(msg, "STD"); break;
                case LOG_LEVEL_MAX:      sprintf(msg, "MAX"); break;
                default:
                    logger_log(LOG_LEVEL_SYSTEM, "Probowano ustawic nieprawidlowy poziom logowania");
                    continue;
            }
            g_loggerState.current_level_factual = current_levele;
            logger_log(LOG_LEVEL_SYSTEM, "Zmieniono status logowania na | [%s]", msg);
        }
    }

    return NULL;
}

void* dump_thread(void *arg) {
    (void)arg;

    sigset_t sigset;
    sigfillset(&sigset);
    sigdelset(&sigset, SIG_MAKE_DUMP);
    pthread_sigmask(SIG_SETMASK, &sigset, NULL);

    while (g_loggerState.is_working_flag) {
        if (sem_wait(&g_loggerState.pending_dump_counter_sem) == 0) {
            if (!g_loggerState.is_working_flag) break;

            char file_name[128];
            snprintf(file_name, sizeof(file_name), "%s_%d.txt", g_loggerState.app_name, g_loggerState.dump_counter + 1);
            g_loggerState.dump_counter++;

            FILE *dump_file = fopen(file_name, "w");
            if (dump_file != NULL) {
                if (g_loggerState.app_dump != NULL) {
                    g_loggerState.app_dump(dump_file);
                }
                fclose(dump_file);

                logger_log(LOG_LEVEL_SYSTEM, "Wykonano zrzut stanu aplikacji do pliku %s", file_name);
            }
        }
    }
    return NULL;
}