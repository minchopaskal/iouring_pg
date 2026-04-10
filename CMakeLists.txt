cmake_minimum_required(VERSION 4.0.0)

project(iouringpg)

add_executable(writetofile wf.c)
target_link_libraries(writetofile
    PRIVATE
    uring
)

add_executable(srv srv.cpp)
target_compile_options(srv PRIVATE -g -ggdb -fsanitize=address -fno-omit-frame-pointer)
target_link_options(srv PRIVATE -fsanitize=address)
target_link_libraries(srv
    PRIVATE
    uring
)

add_executable(client client.c)
target_compile_options(client PRIVATE -g -ggdb -fsanitize=address -fno-omit-frame-pointer)
target_link_options(client PRIVATE -fsanitize=address)
