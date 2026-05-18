from cffi import FFI

ffibuilder = FFI()

ffibuilder.cdef("""
    void dpdk_init(void);
    void dpdk_simulate_packet(const uint8_t* data, size_t len);
    int dpdk_read(uint8_t* out_data);
    void dpdk_cleanup(void);
""")

ffibuilder.set_source("_dpdk_sim",
"""
    #include "src/dpdk_sim.c"
""",
    include_dirs=["src"],
)

if __name__ == "__main__":
    ffibuilder.compile(verbose=True)
