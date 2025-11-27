
include_directories(${PROJECT_SOURCE_DIR}/include)

set(BASE_SOURCE zqp.cc ib.cc)

add_library(zqp STATIC ${BASE_SOURCE})
target_link_libraries(zqp ibverbs rdmacm pthread rt mlx5 tbb profiler)

add_executable(zserver zserver.cc)
target_link_libraries(zserver zqp )

add_executable(zClient zClient.cc)
target_link_libraries(zClient zqp )

add_executable(latency_test latency_test.cc)
target_link_libraries(latency_test zqp )

install(TARGETS zqp
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
        RUNTIME DESTINATION bin
        PUBLIC_HEADER DESTINATION include)
