cmake_minimum_required(VERSION 3.16)
project(mobilegraph)

set(CMAKE_CXX_STANDARD 20)

add_library(mobilegraph library.cpp library.h Pager.cpp Pager.h)

add_executable(mobilegraph_console app.cpp library.cpp Pager.cpp Pager.h)