#define CUB_STDERR
#include <stdio.h>
#include <iostream>
#include <cub/block/block_load.cuh>
#include <cub/block/block_store.cuh>
#include <cub/block/block_reduce.cuh>

using namespace cub;
//---------------------------------------------------------------------
// Kernels
//---------------------------------------------------------------------
#define BLOCK_THREADS 64
#define ITEMS_PER_THREAD 16

extern "C"
__global__ void invokeBlockSumKernel( int num,
    long         *d_in,          // Tile of input
    long         *d_out)         // Tile aggregate
{
    const long ix = threadIdx.x + blockIdx.x * (long)blockDim.x;

    // Specialize BlockReduce type for our thread block
    typedef BlockReduce<int, BLOCK_THREADS, BLOCK_REDUCE_WARP_REDUCTIONS> BlockReduceT;
    // Shared memory
    __shared__ typename BlockReduceT::TempStorage temp_storage;
    // Per-thread tile data
    int data[ITEMS_PER_THREAD];
    LoadDirectStriped<BLOCK_THREADS>(ix, d_in, data);

    // Compute sum
    long aggregate = BlockReduceT(temp_storage).Sum(data);
    if (ix == 0)
    {
       *d_out = aggregate;
    }

}

