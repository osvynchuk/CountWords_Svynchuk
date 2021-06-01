cmake_minimum_required(VERSION 3.5)

project(CountWords_Svynchuk LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(CountWords_Svynchuk main.cpp queue.h)

find_package(Threads REQUIRED)
target_link_libraries(CountWords_Svynchuk stdc++fs)
target_link_libraries(CountWords_Svynchuk Threads::Threads)
