#define BOOST_TEST_MODULE rdma hello world
#include <boost/test/unit_test.hpp>

#include <iostream>
#include <functional>
template<typename T>
using deleted_unique_ptr = std::unique_ptr<T, void(*)(T*)>;

#include <infiniband/verbs.h>


BOOST_AUTO_TEST_CASE(boost_utf_hello_world) {
    BOOST_TEST(true);
}

BOOST_AUTO_TEST_CASE(ibv_hello_world) {
    deleted_unique_ptr<struct ibv_device*> devices(
        ibv_get_device_list(NULL),
        [](struct ibv_device **p) { ibv_free_device_list(p); }
    );
    BOOST_TEST_REQUIRE(devices.get(), "no RDMA devices listed");
    for (size_t i = 0; devices.get()[i] != NULL; i++) {
        const auto &dev = devices.get()[i];
        std::cout << "name " << dev->name << std::endl;
        std::cout << "dev_name " << dev->dev_name << std::endl;
        std::cout << "dev_path " << dev->dev_path << std::endl;
        std::cout << "dev_path " << dev->dev_path << std::endl;
        std::cout << "ibdev_path " << dev->ibdev_path << std::endl;
    }
}
