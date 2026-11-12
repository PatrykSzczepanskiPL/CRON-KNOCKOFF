//
// Created by root on 11/10/26.
//

#include "my_cron.h"


int is_first_instance(int *lock_fd){
    if(!lock_fd){
        return -1;
    }
    int fd = open(CRON_LOCK_FILE, O_RDWR | O_CREAT, 0666);
    if(fd < 0 ){
        printf("nie można otworzyć / utworzyć pliku zamka\n");
        return -1;
    }

    struct flock lock_struct;
    lock_struct.l_type = F_WRLCK;
    lock_struct.l_whence = SEEK_SET;
    lock_struct.l_start = 0;
    lock_struct.l_len = 0;

    if (fcntl(fd, F_SETLK, &lock_struct) < 0) {
        if (errno == EACCES || errno == EAGAIN) {
            close(fd);
            return 0;
        }
        close(fd);
        return -1;
    }

    *lock_fd = fd;
    return 1;

}


/*
 * MODUŁ SERWERA
 */

// Inicjalizacja i pętla główna serwera
int server_run(int lock_fd){
    if(lock_fd == -1){
        return -1;
    }
    cron_server_state_t serverState;
    cron_msg_t req = {0};
    cron_msg_t res = {0};

    if(server_init(&serverState) != 0){
        return -1;
    }
    serverState.lock_fd = lock_fd;
    printf("STARTED CRON SERVER\n");
    fflush(stdin);
    while(serverState.is_running){
        memset(&req, 0, MSG_SIZE);
        memset(&res, 0, MSG_SIZE);

        if(mq_receive(serverState.server_mq, (char *)&req, MSG_SIZE, NULL) != MSG_SIZE){
            //TODO: logowanie odczytania niepoprawnej wiadomości
            continue;
        }

        res.cmd_type = CMD_RESPONSE;
        res.client_pid = req.client_pid;

        switch (req.cmd_type) {
            case CMD_ADD_TASK: {
                int new_id = task_manager_add(&serverState, &req.task_data);
                res.status = (new_id > 0) ? RES_OK : RES_ERROR;
                res.target_task_id = (new_id > 0) ? (uint32_t)new_id : 0;
                break;
            }
            case CMD_CANCEL_TASK: {
                int err = task_manager_cancel(&serverState, req.target_task_id);
                res.status = (err == 0) ? RES_OK : RES_ERROR;
                res.target_task_id = req.target_task_id;
                break;
            }
            case CMD_LIST_TASKS: {
                int err = task_manager_list(&serverState, &res);
                res.status = (err == 0) ? RES_OK : RES_ERROR;
                break;
            }
            case CMD_STOP_SERVER:
                serverState.is_running = 0;
                res.status = RES_OK;
                break;

            default:
                res.status = RES_ERROR;
                continue;
        }

        ipc_server_send_response(&res);
    }


    task_manager_cancel_all(&serverState);
    server_cleanup(&serverState);
    return 0;
}
int server_init(cron_server_state_t *state){
    sigset_t block_all_set;
    sigfillset(&block_all_set);
    pthread_sigmask(SIG_SETMASK, &block_all_set, NULL);
    memset(state, 0, sizeof(cron_server_state_t));

    for(int i = 0; i<MAX_TASKS; i++){
        state->tasks[i].is_active = 0;
    }

    state->next_task_id = 0;
    state->is_running = 1;

    pthread_mutex_init(&state->tasks_mutex, NULL);



    struct mq_attr attr = {.mq_msgsize = MSG_SIZE, .mq_maxmsg = 10, .mq_flags = 0, .mq_curmsgs = 0};
    state->server_mq = mq_open(CRON_SERVER_QUEUE, O_RDWR |O_CREAT |O_EXCL, 0666, &attr);
    if(state->server_mq == -1){
        pthread_mutex_destroy(&state->tasks_mutex);
        return -1;
    }



    pthread_create(&state->signal_thread,NULL, server_signal_thread,(void *)state);
    return 0;
}

// Sprzątanie zasobów serwera przy zamknięciu (wyrejestrowanie timerów, kolejki, logera)
void server_cleanup(cron_server_state_t *state){
    state->is_running = 0;
    pthread_cancel(state->signal_thread);
    pthread_join(state->signal_thread, NULL);
    mq_close(state->server_mq);
    mq_unlink(CRON_SERVER_QUEUE);
    memset(state->tasks, 0, sizeof(cron_task_t) * MAX_TASKS);
    state->next_task_id = -1;
    pthread_mutex_destroy(&state->tasks_mutex);
    close(state->lock_fd);
    printf("\nTERMINATED CRON SERVER\n");
    fflush(stdin);
}

//wątek sygnałów
void * server_signal_thread(void * args){
    cron_server_state_t * serverStatePtr = (cron_server_state_t *)args;
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGINT);
    sigaddset(&set, SIGTERM);
    int recieved_signal_number = -1;
    if(sigwait(&set, &recieved_signal_number) == 0){
        cron_msg_t wake_up_msg;
        memset(&wake_up_msg, 0, MSG_SIZE);
        wake_up_msg.cmd_type = CMD_STOP_SERVER;
        mq_send(serverStatePtr->server_mq, (char *)&wake_up_msg, MSG_SIZE, 0);
    }


    return NULL;
}


//dodanie zadania do listy sygnałów


int task_manager_add(cron_server_state_t *state, cron_task_t *new_task){
    if(!state || !new_task || ((new_task->sched_type == SCHEDULE_PERIODIC || new_task->sched_type == SCHEDULE_RELATIVE) && new_task->interval_sec <=0)){
        return -1;
    }

    cron_task_t * free_task_slot = NULL;

    pthread_mutex_lock(&state->tasks_mutex);

    for (int i = 0; i < MAX_TASKS; ++i) {
        if(state->tasks[i].is_active == 0){
            free_task_slot = &state->tasks[i];
            break;
        }
    }
    if(free_task_slot == NULL){
        pthread_mutex_unlock(&state->tasks_mutex);
        return -2;
    }
    memcpy(free_task_slot, new_task, sizeof(cron_task_t));
    free_task_slot->task_id = ++state->next_task_id;
    free_task_slot->is_active = 1;
    free_task_slot->server_state = (struct cron_server_state_t *)state;
    struct sigevent sev;
    memset(&sev, 0, sizeof(struct sigevent));
    sev.sigev_notify = SIGEV_THREAD;
    sev.sigev_notify_function = task_execution_handler;
    sev.sigev_notify_attributes = NULL;
    sev.sigev_value.sival_ptr = (void *)free_task_slot;

    if(timer_create(CLOCK_REALTIME, &sev, &free_task_slot->posix_timer_id) !=0){
        free_task_slot->is_active = 0;
        pthread_mutex_unlock(&state->tasks_mutex);
        return -3;
    }

    struct itimerspec ts;
    memset(&ts, 0, sizeof(ts));
    int flags = 0; // Domyślnie czas względny

    switch (free_task_slot->sched_type) {
        case SCHEDULE_RELATIVE:
            ts.it_value.tv_sec = free_task_slot->interval_sec;
            ts.it_value.tv_nsec = 0;
            ts.it_interval.tv_sec = 0;
            ts.it_interval.tv_nsec = 0;
            break;

        case SCHEDULE_ABSOLUTE:
            ts.it_value.tv_sec = free_task_slot->target_time;
            ts.it_value.tv_nsec = 0;
            ts.it_interval.tv_sec = 0;
            ts.it_interval.tv_nsec = 0;
            flags = TIMER_ABSTIME;
            break;

        case SCHEDULE_PERIODIC:
            ts.it_value.tv_sec = free_task_slot->interval_sec;
            ts.it_value.tv_nsec = 0;
            ts.it_interval.tv_sec = free_task_slot->interval_sec;
            ts.it_interval.tv_nsec = 0;
            break;
    }

    if(timer_settime(free_task_slot->posix_timer_id,flags,&ts,NULL) != 0){
        timer_delete(free_task_slot->posix_timer_id);
        free_task_slot->is_active = false;
        pthread_mutex_unlock(&state->tasks_mutex);
        return -4;
    }



    int task_id = (int)free_task_slot->task_id;
    pthread_mutex_unlock(&state->tasks_mutex);

    return task_id;
}

int task_manager_cancel(cron_server_state_t *state, uint32_t task_id) {
    if (!state || task_id == 0) return -1;

    pthread_mutex_lock(&state->tasks_mutex);

    for (int i = 0; i < MAX_TASKS; ++i) {
        if (state->tasks[i].is_active && state->tasks[i].task_id == task_id) {
            timer_delete(state->tasks[i].posix_timer_id);

            memset(&state->tasks[i], 0, sizeof(cron_task_t));
            state->tasks[i].is_active = false;

            pthread_mutex_unlock(&state->tasks_mutex);
            return 0;
        }
    }

    pthread_mutex_unlock(&state->tasks_mutex);
    return -1;
}

int task_manager_list(cron_server_state_t *state, cron_msg_t *res) {
    if (!state || !res) {
        return -1;
    }

    char client_queue_name[64];
    sprintf(client_queue_name, CRON_CLIENT_QUEUE_FMT, res->client_pid);

    mqd_t client_mq = mq_open(client_queue_name, O_WRONLY);
    if (client_mq == (mqd_t)-1) {
        return -1;
    }

    pthread_mutex_lock(&state->tasks_mutex);

    cron_task_t *tasks = state->tasks;

    for (int i = 0; i < MAX_TASKS; i++) {
        if (tasks[i].is_active) {
            res->status = RES_PART;

            char scheduled_type_buff[16];
            char time_buff[32];

            switch (tasks[i].sched_type) {
                case SCHEDULE_ABSOLUTE: {
                    sprintf(scheduled_type_buff, "ABSOLUTE");
                    struct tm readable_time;

                    if (localtime_r(&tasks[i].target_time, &readable_time) != &readable_time) {
                        pthread_mutex_unlock(&state->tasks_mutex);
                        mq_close(client_mq);
                        return -1;
                    }
                    sprintf(time_buff, "%02d:%02d", readable_time.tm_hour, readable_time.tm_min);
                    break;
                }
                case SCHEDULE_PERIODIC: {
                    sprintf(scheduled_type_buff, "PERIODIC");
                    sprintf(time_buff, "%u s", tasks[i].interval_sec);
                    break;
                }
                case SCHEDULE_RELATIVE: {
                    sprintf(scheduled_type_buff, "RELATIVE");
                    sprintf(time_buff, "%u s", tasks[i].interval_sec);
                    break;
                }
                default: {
                    sprintf(scheduled_type_buff, "ERROR");
                    sprintf(time_buff, "?????");
                    break;
                }
            }

            sprintf(res->response_payload, "%u\t|\t%s\t|\t%s\t|\t%s %s\n",
                    tasks[i].task_id,
                    scheduled_type_buff,
                    time_buff,
                    tasks[i].command,
                    tasks[i].args);

            if (mq_send(client_mq, (const char *)res, MSG_SIZE, 0) == -1) {
                pthread_mutex_unlock(&state->tasks_mutex);
                mq_close(client_mq);
                return -1;
            }
        }
    }

    pthread_mutex_unlock(&state->tasks_mutex);

    res->status = RES_OK;
    res->response_payload[0] = '\0';

    mq_close(client_mq);
    return 0;
}


int task_manager_cancel_all(cron_server_state_t *state){
    if (!state) return -1;

    pthread_mutex_lock(&state->tasks_mutex);

    for (int i = 0; i < MAX_TASKS; ++i) {
        if (state->tasks[i].is_active) {
            timer_delete(state->tasks[i].posix_timer_id);
            memset(&state->tasks[i], 0, sizeof(cron_task_t));
            state->tasks[i].is_active = false;
        }
    }

    pthread_mutex_unlock(&state->tasks_mutex);
    return 0;
}

extern char **environ;

void task_execution_handler(union sigval sv) {
    cron_task_t *task = (cron_task_t *)sv.sival_ptr;
    if (!task) return;

    char full_cmd[MAX_CMD_LEN + MAX_ARGS_LEN + 2];

    if (task->args[0] != '\0') {
        sprintf(full_cmd, "%s %s", task->command, task->args);
    } else {
        sprintf(full_cmd, "%s", task->command);
    }

    char *const argv[] = {(char *)"sh", (char *)"-c", full_cmd, NULL};
    pid_t pid;

    if (posix_spawnp(&pid, "sh", NULL, NULL, argv, environ) == 0) {
        waitpid(pid, NULL, 0); //
    }

    if (task->sched_type != SCHEDULE_PERIODIC) {
        cron_server_state_t *state = (cron_server_state_t *)task->server_state;
        if (state) {
            task_manager_cancel(state, task->task_id);
        }
    }
}




static void print_help(const char *prog_name) {
    printf("Użycie: %s [OPCJE]\n", prog_name);
    printf("Opcje:\n");
    printf("  -a <komenda>     Ścieżka/polecenie do uruchomienia\n");
    printf("  -g <argumenty>   Argumenty dla polecenia (opcjonalnie)\n");
    printf("  -t <czas>        Czas wyzwolenia (+10s, *5s, HH:MM)\n");
    printf("  -l               Wyświetla listę aktywnych zadań\n");
    printf("  -c <task_id>     Anuluje zadanie o podanym ID\n");
    printf("  -s               Zatrzymuje usługę serwera\n");
    printf("  -h               Wyświetla tę pomoc\n");
}



int client_run(int argc, char *argv[]) {
    if (argc <= 1 || !argv) {
        print_help(argv[0]);
        return -1;
    }

    cron_msg_t req;
    cron_msg_t res;
    memset(&req, 0, sizeof(cron_msg_t));
    memset(&res, 0, sizeof(cron_msg_t));

    req.client_pid = getpid();

    int opt;
    char time_str[64] = {0};
    int action_count = 0;

    optind = 1;

    while ((opt = getopt(argc, argv, "a:g:t:c:lsh")) != -1) {
        switch (opt) {
            case 'a':
                req.cmd_type = CMD_ADD_TASK;
                strncpy(req.task_data.command, optarg, MAX_CMD_LEN - 1);
                action_count++;
                break;

            case 'g':
                strncpy(req.task_data.args, optarg, MAX_ARGS_LEN - 1);
                break;

            case 't':
                strncpy(time_str, optarg, sizeof(time_str) - 1);
                break;

            case 'l':
                req.cmd_type = CMD_LIST_TASKS;
                action_count++;
                break;

            case 'c':
                req.cmd_type = CMD_CANCEL_TASK;
                req.target_task_id = (uint32_t)atoi(optarg);
                action_count++;
                break;

            case 's':
                req.cmd_type = CMD_STOP_SERVER;
                action_count++;
                break;

            case 'h':
                print_help(argv[0]);
                return 0;

            default:
                print_help(argv[0]);
                return -1;
        }
    }

    if (action_count == 0) {
        print_help(argv[0]);
        return -1;
    }

    if (action_count > 1) {
        return -1;
    }

    if (req.cmd_type == CMD_ADD_TASK) {
        if (strlen(time_str) == 0) {
            return -1;
        }
        if (parse_schedule_args(time_str, &req.task_data.sched_type,
                                &req.task_data.target_time,
                                &req.task_data.interval_sec) != 0) {
            return -1;
        }
    }

    if (ipc_client_send_and_receive(&req, &res) != 0) {
        return -1;
    }

    if (res.response_payload[0] != '\0') {
        printf("%s", res.response_payload);
    }

    return (res.status == RES_OK) ? 0 : -1;
}

int parse_schedule_args(const char *time_str, schedule_type_t *type, time_t *target_time, uint32_t *interval) {
    if (!time_str || !type || !target_time || !interval || time_str[0] == '\0') {
        return -1;
    }

    *target_time = 0;
    *interval = 0;

    switch (time_str[0]) {
        case '*': {
            int val = atoi(time_str + 1);
            if (val <= 0) return -1;
            *type = SCHEDULE_PERIODIC;
            *interval = (uint32_t)val;
            break;
        }
        case '+': {
            int val = atoi(time_str + 1);
            if (val <= 0) return -1;
            *type = SCHEDULE_RELATIVE;
            *interval = (uint32_t)val;
            break;
        }
        default: {
            *type = SCHEDULE_ABSOLUTE;
            int hours = 0, mins = 0;

            if (sscanf(time_str, "%d:%d", &hours, &mins) == 2) {
                if (hours < 0 || hours > 23 || mins < 0 || mins > 59) {
                    return -1;
                }

                time_t now = time(NULL);
                struct tm tm_now;
                localtime_r(&now, &tm_now);

                tm_now.tm_hour = hours;
                tm_now.tm_min = mins;
                tm_now.tm_sec = 0;

                time_t target = mktime(&tm_now);

                if (target <= now) {
                    target += 86400;
                }
                *target_time = target;
            } else {
                long long ts = atoll(time_str);
                if (ts <= 0) return -1;
                *target_time = (time_t)ts;
            }
            break;
        }
    }

    return 0;
}




int ipc_client_send_and_receive(cron_msg_t *req, cron_msg_t *res) {
    if (!req || !res) return -1;

    char client_queue_name[64];
    snprintf(client_queue_name, sizeof(client_queue_name), CRON_CLIENT_QUEUE_FMT, getpid());

    struct mq_attr attr = {
            .mq_flags = 0,
            .mq_maxmsg = 10,
            .mq_msgsize = MSG_SIZE,
            .mq_curmsgs = 0
    };

    mqd_t client_mq = mq_open(client_queue_name, O_RDONLY | O_CREAT | O_EXCL, 0666, &attr);
    if (client_mq == (mqd_t)-1) return -1;

    mqd_t server_mq = mq_open(CRON_SERVER_QUEUE, O_WRONLY);
    if (server_mq == (mqd_t)-1) {
        mq_close(client_mq);
        mq_unlink(client_queue_name);
        return -1;
    }

    if (mq_send(server_mq, (const char *)req, MSG_SIZE, 0) == -1) {
        mq_close(server_mq);
        mq_close(client_mq);
        mq_unlink(client_queue_name);
        return -1;
    }

    while (1) {
        if (mq_receive(client_mq, (char *)res, MSG_SIZE, NULL) == -1) {
            mq_close(server_mq);
            mq_close(client_mq);
            mq_unlink(client_queue_name);
            return -1;
        }

        if (res->response_payload[0] != '\0') {
            printf("%s", res->response_payload);
        }

        if (res->status != RES_PART) {
            break;
        }
    }

    mq_close(server_mq);
    mq_close(client_mq);
    mq_unlink(client_queue_name);
    return (res->status == RES_OK) ? 0 : -1;
}

int ipc_server_send_response(cron_msg_t *res){

    char client_queue_name[64];
    sprintf(client_queue_name, CRON_CLIENT_QUEUE_FMT, res->client_pid);
    mqd_t client_mq = mq_open(client_queue_name,O_WRONLY);
    if(client_mq == -1){
        return -1;
    }

    if(mq_send(client_mq,(const char *)res, MSG_SIZE,0) == -1){
        mq_close(client_mq);
        return -1;
    }

    mq_close(client_mq);
    return 0;
}
