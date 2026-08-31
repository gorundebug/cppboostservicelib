cmake_minimum_required(VERSION 3.24)

project(cppboostservicelib_consumer LANGUAGES CXX)

find_package(cppboostservicelib REQUIRED)

add_executable(cppboostservicelib_consumer main.cpp)
target_link_libraries(cppboostservicelib_consumer PRIVATE
    servicelib::servicelib servicelib::cron)
