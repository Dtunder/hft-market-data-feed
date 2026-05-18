#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define RING_SIZE 1024
#define PACKET_SIZE 64

typedef struct {
    uint32_t head;
    uint32_t tail;
    uint8_t buffer[RING_SIZE][PACKET_SIZE];
} dpdk_ring_t;

static dpdk_ring_t* rx_ring = NULL;

void dpdk_init(void) {
    if (!rx_ring) {
        rx_ring = (dpdk_ring_t*)malloc(sizeof(dpdk_ring_t));
        rx_ring->head = 0;
        rx_ring->tail = 0;
    }
}

void dpdk_simulate_packet(const uint8_t* data, size_t len) {
    if (!rx_ring) return;
    uint32_t next_head = (rx_ring->head + 1) % RING_SIZE;
    if (next_head != rx_ring->tail) {
        size_t copy_len = len < PACKET_SIZE ? len : PACKET_SIZE;
        memcpy(rx_ring->buffer[rx_ring->head], data, copy_len);
        rx_ring->head = next_head;
    }
}

int dpdk_read(uint8_t* out_data) {
    if (!rx_ring || rx_ring->head == rx_ring->tail) {
        return 0;
    }
    memcpy(out_data, rx_ring->buffer[rx_ring->tail], PACKET_SIZE);
    rx_ring->tail = (rx_ring->tail + 1) % RING_SIZE;
    return 1;
}

void dpdk_cleanup(void) {
    if (rx_ring) {
        free(rx_ring);
        rx_ring = NULL;
    }
}
