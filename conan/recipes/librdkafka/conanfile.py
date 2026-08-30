import os

from conan import ConanFile
from conan.tools.env import VirtualBuildEnv
from conan.tools.files import chdir, copy, get
from conan.tools.gnu import Autotools, AutotoolsDeps, AutotoolsToolchain
from conan.tools.layout import basic_layout
from conan.tools.microsoft import is_msvc
from conan.tools.gnu import PkgConfigDeps


required_conan_version = ">=2.8.0"


class LibrdkafkaConan(ConanFile):
    name = "librdkafka"
    description = "The Apache Kafka C/C++ client library"
    license = "BSD-2-Clause"
    homepage = "https://github.com/confluentinc/librdkafka"
    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "zlib": [True, False],
        "zstd": [True, False],
        "ssl": [True, False],
        "sasl": [True, False],
        "curl": [True, False],
        "syslog": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "zlib": False,
        "zstd": False,
        "ssl": False,
        "sasl": False,
        "curl": False,
        "syslog": False,
    }

    @property
    def _depends_on_cyrus_sasl(self):
        return bool(self.options.sasl) and self.settings.os != "Windows"

    def config_options(self):
        if self.settings.os == "Windows":
            self.options.rm_safe("fPIC")
        if is_msvc(self):
            self.options.rm_safe("syslog")

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def layout(self):
        basic_layout(self, src_folder="src")

    def requirements(self):
        self.requires("lz4/1.9.4")
        if self.options.zlib:
            self.requires("zlib/[>=1.2.11 <2]")
        if self.options.zstd:
            self.requires("zstd/1.5.5")
        if self.options.ssl:
            self.requires("openssl/[>=1.1 <4]")
        if self._depends_on_cyrus_sasl:
            self.requires("cyrus-sasl/2.1.28")
        if self.options.curl:
            self.requires("libcurl/[>=7.78.0 <9]")

    def build_requirements(self):
        if self._depends_on_cyrus_sasl:
            self.tool_requires("pkgconf/2.1.0")

    def source(self):
        source = self.conan_data["sources"][str(self.version)]
        base = (os.getenv("DEPENDENCY_GITHUB_RAW_URL") or "https://github.com").rstrip("/")
        get(
            self,
            url=f"{base}/confluentinc/librdkafka/archive/refs/tags/v{self.version}.tar.gz",
            sha256=source["sha256"],
            strip_root=True,
        )

    def generate(self):
        VirtualBuildEnv(self).generate()
        AutotoolsDeps(self).generate()
        PkgConfigDeps(self).generate()

        toolchain = AutotoolsToolchain(self)
        args = [
            "--enable-static",
            "--enable-lz4-ext",
            "--enable-zlib" if self.options.zlib else "--disable-zlib",
            "--enable-zstd" if self.options.zstd else "--disable-zstd",
            "--enable-ssl" if self.options.ssl else "--disable-ssl",
            "--enable-sasl" if self.options.sasl else "--disable-sasl",
            "--enable-curl" if self.options.curl else "--disable-curl",
            "--enable-syslog" if self.options.get_safe("syslog", False) else "--disable-syslog",
        ]
        sanitizer = self.settings.compiler.get_safe("sanitizer")
        if sanitizer == "Thread":
            # This is librdkafka's supported TSan profile. Its configure script
            # documents that glibc C11 threads are incompatible with TSan and
            # selects the bundled pthread-backed tinycthread implementation.
            args.append("--disable-c11threads")
        # librdkafka uses mklove rather than GNU Autoconf. It supports the
        # standard install directories except oldincludedir, which Conan's
        # generic Autotools toolchain adds unconditionally.
        toolchain.configure_args = [
            arg for arg in toolchain.configure_args
            if not arg.startswith("--oldincludedir=")
        ]
        toolchain.configure_args.extend(args)
        toolchain.generate()

    def build(self):
        # librdkafka's upstream configure wrapper sources mklove modules using
        # paths relative to the current directory. Build in Conan's private
        # source copy, as upstream documents, rather than invoking it from an
        # out-of-tree working directory.
        with chdir(self, self.source_folder):
            autotools = Autotools(self)
            autotools.configure()
            autotools.make(target="librdkafka.a", args=["-C", "src"])
            autotools.make(target="librdkafka++.a", args=["-C", "src-cpp"])

    def package(self):
        copy(self, "LICENSE*", src=self.source_folder, dst=os.path.join(self.package_folder, "licenses"))
        include_dir = os.path.join(self.package_folder, "include", "librdkafka")
        lib_dir = os.path.join(self.package_folder, "lib")
        copy(self, "rdkafka.h", src=os.path.join(self.source_folder, "src"), dst=include_dir)
        copy(self, "rdkafka_mock.h", src=os.path.join(self.source_folder, "src"), dst=include_dir)
        copy(self, "rdkafkacpp.h", src=os.path.join(self.source_folder, "src-cpp"), dst=include_dir)
        copy(self, "librdkafka.a", src=os.path.join(self.source_folder, "src"), dst=lib_dir)
        copy(self, "librdkafka++.a", src=os.path.join(self.source_folder, "src-cpp"), dst=lib_dir)

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "RdKafka")
        self.cpp_info.set_property("cmake_target_name", "RdKafka::rdkafka++")
        self.cpp_info.set_property("pkg_config_name", "rdkafka++")

        c_api = self.cpp_info.components["rdkafka"]
        c_api.set_property("cmake_target_name", "RdKafka::rdkafka")
        c_api.set_property("pkg_config_name", "rdkafka")
        c_api.libs = ["rdkafka"]
        c_api.requires = ["lz4::lz4"]
        if self.options.zlib:
            c_api.requires.append("zlib::zlib")
        if self.options.zstd:
            c_api.requires.append("zstd::zstd")
        if self.options.ssl:
            c_api.requires.append("openssl::openssl")
        if self._depends_on_cyrus_sasl:
            c_api.requires.append("cyrus-sasl::cyrus-sasl")
        if self.options.curl:
            c_api.requires.append("libcurl::libcurl")
        if self.settings.os == "Windows":
            c_api.system_libs = ["ws2_32", "secur32"]
        elif self.settings.os in ("Linux", "FreeBSD"):
            c_api.system_libs = ["pthread", "rt", "dl", "m"]
        if not self.options.shared:
            c_api.defines.append("LIBRDKAFKA_STATICLIB")

        cpp_api = self.cpp_info.components["rdkafka++"]
        cpp_api.set_property("cmake_target_name", "RdKafka::rdkafka++")
        cpp_api.set_property("pkg_config_name", "rdkafka++")
        cpp_api.libs = ["rdkafka++"]
        cpp_api.requires = ["rdkafka"]
