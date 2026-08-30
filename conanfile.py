from conan import ConanFile
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import copy
from conan.errors import ConanInvalidConfiguration
import os
import sys
from pathlib import Path
import re

sys.path.insert(0, str(Path(__file__).resolve().parent / "conan"))
from dependencies_generated import VERSIONS


required_conan_version = ">=2.8.0"
LOCAL_RECIPE_NAMESPACE = "@gorundebug/boost"


class CppBoostServiceLibConan(ConanFile):
    name = "cppboostservicelib"
    package_type = "library"
    license = "Apache-2.0"
    url = "https://github.com/gorundebug/cppboostservicelib"
    description = "ServiceLib runtime implemented with Boost.Asio"
    settings = "os", "arch", "compiler", "build_type"
    exports = "conan/dependencies_generated.py"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "with_grpc": [True, False],
        "with_kafka": [True, False],
        "with_otel": [True, False],
        "with_cron": [True, False],
        "with_tests": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "with_grpc": False,
        "with_kafka": False,
        "with_otel": False,
        "with_cron": False,
        "with_tests": False,
    }
    exports_sources = (
        "CMakeLists.txt",
        "cmake/*",
        "include/*",
        "src/*",
        "tests/*",
    )

    def set_version(self):
        cmake = (Path(self.recipe_folder) / "CMakeLists.txt").read_text()
        match = re.search(
            r"project\(cppboostservicelib VERSION ([0-9]+\.[0-9]+\.[0-9]+)",
            cmake,
        )
        if match is None:
            raise ConanInvalidConfiguration(
                "cppboostservicelib version is missing from CMakeLists.txt"
            )
        self.version = match.group(1)

    def config_options(self):
        if self.settings.os == "Windows":
            self.options.rm_safe("fPIC")

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

        # asio-grpc 3.5.0 declares Boost 1.88 and gRPC declares Protobuf 5.27.
        # The framework contract intentionally pins newer compatible versions;
        # requirements() records the corresponding Conan overrides explicitly.
        self.options["boost"].shared = False
        # Conan's Boost CMake package exports every declared component target.
        # Keep the test library present even though ServiceLib itself only asks
        # for Boost.JSON; otherwise loading BoostConfig.cmake fails while it
        # validates the exported Boost::unit_test_framework target.
        self.options["boost"].without_test = False
        self.options["grpc"].shared = False
        self.options["grpc"].codegen = True
        self.options["asio-grpc"].backend = "boost"
        self.options["librdkafka"].shared = False
        self.options["librdkafka"].ssl = True
        self.options["librdkafka"].sasl = True
        self.options["librdkafka"].zlib = True
        self.options["opentelemetry-cpp"].shared = False
        self.options["opentelemetry-cpp"].with_otlp_grpc = True
        self.options["opentelemetry-cpp"].with_otlp_http = False
        self.options["opentelemetry-cpp"].with_zipkin = False

    def requirements(self):
        self.requires(f"boost/{VERSIONS['boost']}", override=True)
        self.requires(f"yaml-cpp/{VERSIONS['yaml-cpp']}")

        if self.options.with_tests:
            self.requires(f"gtest/{VERSIONS['googletest']}{LOCAL_RECIPE_NAMESPACE}")

        if self.options.with_grpc or self.options.with_otel:
            self.requires(f"protobuf/{VERSIONS['protobuf']}", override=True)
            self.requires(f"grpc/{VERSIONS['grpc']}{LOCAL_RECIPE_NAMESPACE}", override=True)
            self.requires(f"asio-grpc/{VERSIONS['asio-grpc']}")

        if self.options.with_kafka:
            self.requires(f"librdkafka/{VERSIONS['librdkafka']}@gorundebug/boost")

        if self.options.with_otel:
            self.requires(f"opentelemetry-cpp/{VERSIONS['opentelemetry-cpp']}{LOCAL_RECIPE_NAMESPACE}")

        if self.options.with_cron:
            self.requires(f"libcron/{VERSIONS['libcron']}{LOCAL_RECIPE_NAMESPACE}")

    def build_requirements(self):
        if self.options.with_grpc or self.options.with_otel:
            self.tool_requires(f"protobuf/{VERSIONS['protobuf']}", override=True)
        if self.options.with_otel:
            self.tool_requires(f"grpc/{VERSIONS['grpc']}{LOCAL_RECIPE_NAMESPACE}", override=True)

    def validate(self):
        if self.settings.get_safe("compiler.cppstd"):
            check_min_cppstd(self, "20")
        if self.options.with_otel and not self.options.with_grpc:
            raise ConanInvalidConfiguration("with_otel requires with_grpc")

    def layout(self):
        cmake_layout(self)

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()

        toolchain = CMakeToolchain(self)
        toolchain.cache_variables["CPPBOOSTSERVICELIB_DEPENDENCY_MODE"] = "CONAN"
        toolchain.cache_variables["CPPBOOSTSERVICELIB_BUILD_TESTS"] = bool(
            self.options.with_tests
        )
        toolchain.cache_variables["CPPBOOSTSERVICELIB_ENABLE_GRPC"] = bool(
            self.options.with_grpc
        )
        toolchain.cache_variables["CPPBOOSTSERVICELIB_ENABLE_KAFKA"] = bool(
            self.options.with_kafka
        )
        toolchain.cache_variables["CPPBOOSTSERVICELIB_ENABLE_OTEL"] = bool(
            self.options.with_otel
        )
        toolchain.cache_variables["CPPBOOSTSERVICELIB_ENABLE_CRON"] = bool(
            self.options.with_cron
        )
        toolchain.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
        if self.options.with_tests:
            cmake.test()

    def package(self):
        copy(self, "LICENSE*", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        cmake = CMake(self)
        cmake.install()

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "cppboostservicelib")
        self.cpp_info.set_property("cmake_target_name", "servicelib::servicelib")
        self.cpp_info.builddirs = ["lib/cmake/cppboostservicelib"]
