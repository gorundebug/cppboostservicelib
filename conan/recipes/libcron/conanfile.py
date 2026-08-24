from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, cmake_layout
from conan.tools.files import copy, get, save
import os


required_conan_version = ">=2.8.0"


class LibcronConan(ConanFile):
    name = "libcron"
    version = "1.3.3"
    package_type = "static-library"
    license = "MIT"
    homepage = "https://github.com/PerMalmberg/libcron"
    settings = "os", "arch", "compiler", "build_type"
    options = {"fPIC": [True, False]}
    default_options = {"fPIC": True}
    def config_options(self):
        if self.settings.os == "Windows":
            self.options.rm_safe("fPIC")

    def layout(self):
        cmake_layout(self)

    def source(self):
        base = (os.getenv("SERVICEGEN_GITHUB_RAW_URL") or "https://github.com").rstrip("/")
        get(
            self,
            url=f"{base}/PerMalmberg/libcron/archive/refs/tags/v1.3.3.tar.gz",
            sha256="7d413b7950c82b54157b2a7f446e1e660bd718e542e2ffd3f8715e467ab2b825",
            strip_root=True,
        )
        get(
            self,
            url=f"{base}/HowardHinnant/date/archive/f94b8f36c6180be0021876c4a397a054fe50c6f2.tar.gz",
            sha256="8be4c3a52d99b22a4478ce3e2a23fa4b38587ea3d3bc3d1a4d68de22c2e65fb2",
            destination=os.path.join(self.source_folder, "libcron", "externals", "date"),
            strip_root=True,
        )
        save(
            self,
            os.path.join(self.source_folder, "CMakeLists.txt"),
            """cmake_minimum_required(VERSION 3.24)
project(libcron LANGUAGES CXX)
add_library(libcron STATIC
  libcron/src/CronClock.cpp
  libcron/src/CronData.cpp
  libcron/src/CronRandomization.cpp
  libcron/src/CronSchedule.cpp
  libcron/src/Task.cpp)
target_compile_features(libcron PUBLIC cxx_std_17)
target_compile_definitions(libcron PRIVATE HAS_UNCAUGHT_EXCEPTIONS)
target_include_directories(libcron PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/libcron/include>
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/libcron/externals/date/include>
  $<INSTALL_INTERFACE:include>)
set_target_properties(libcron PROPERTIES POSITION_INDEPENDENT_CODE ${CMAKE_POSITION_INDEPENDENT_CODE})
install(TARGETS libcron ARCHIVE DESTINATION lib)
install(DIRECTORY libcron/include/libcron DESTINATION include)
install(DIRECTORY libcron/externals/date/include/date DESTINATION include)
""",
        )

    def generate(self):
        toolchain = CMakeToolchain(self)
        toolchain.variables["CMAKE_POSITION_INDEPENDENT_CODE"] = self.options.get_safe(
            "fPIC", True
        )
        toolchain.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(self, "LICENSE*", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "libcron")
        self.cpp_info.set_property("cmake_target_name", "libcron::libcron")
        self.cpp_info.libs = ["libcron"]
