cmake_minimum_required(VERSION 3.20)
project(my_cron C)

set(CMAKE_C_STANDARD 11)

add_executable(my_cron main.c my_cron.c my_cron.h logger/logger.c logger/logger.h)

target_link_libraries(my_cron -pthread -lrt)