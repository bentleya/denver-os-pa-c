/*
 * Created by Ivo Georgiev on 2/9/16.
 */

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;



/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;



/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;



/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status
        _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                           size_t size,
                           node_pt node);
static alloc_status
        _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                size_t size,
                                node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);



/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init()
{
    // ensure that it's called only once until mem_free
    // allocate the pool store with initial capacity
    // note: holds pointers only, other functions to allocate/deallocate

    if (pool_store == NULL)
    {
        pool_store = (pool_mgr_pt*) calloc(MEM_POOL_STORE_INIT_CAPACITY, sizeof(pool_mgr_pt));
        pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
        pool_store_size = 0;
    }

    else
        return ALLOC_CALLED_AGAIN;

    if (pool_store != NULL)
        return ALLOC_OK;

    else
        return ALLOC_FAIL;
}

alloc_status mem_free()
{
    // ensure that it's called only once for each mem_init
    // make sure all pool managers have been deallocated
    // can free the pool store array
    // update static variables

    if(pool_store != NULL)
        mem_pool_close(&pool_store[0]->pool);
    else
        return ALLOC_CALLED_AGAIN;

    pool_store_size = 0;
    pool_store_capacity = 0;
    free(pool_store);
    pool_store = NULL;

    if (pool_store == NULL)
        return ALLOC_OK;

    else
        return ALLOC_NOT_FREED;
}

pool_pt mem_pool_open(size_t size, alloc_policy policy)
{
    // make sure there the pool store is allocated
    if(pool_store == NULL)
        return NULL;

    // expand the pool store, if necessary
    if (((float) pool_store_size / pool_store_capacity) > MEM_POOL_STORE_FILL_FACTOR)
    if(_mem_resize_pool_store() != ALLOC_OK)
        return NULL;

    // allocate a new mem pool mgr
    pool_mgr_pt pool_mgr = (pool_mgr_pt) malloc(sizeof(pool_mgr_t));

    // check success, on error return null
    if (pool_mgr == NULL)
        return NULL;

    // allocate a new memory pool
    pool_mgr->pool.mem = (char*) malloc(size);

    // check success, on error deallocate mgr and return null
    if (pool_mgr->pool.mem == NULL)
    {
        free(pool_mgr);
        return NULL;
    }

    // allocate a new node heap
    pool_mgr->node_heap = (node_pt) calloc (MEM_NODE_HEAP_INIT_CAPACITY ,sizeof(node_t));
    pool_mgr->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;

    // check success, on error deallocate mgr/pool and return null
    if (pool_mgr->node_heap == NULL)
    {
        free(pool_mgr);
        return NULL;
    }

    // allocate a new gap index
    pool_mgr->gap_ix = (gap_pt) calloc (MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));
    pool_mgr->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;

    // check success, on error deallocate mgr/pool/heap and return null
    if (pool_mgr->gap_ix == NULL)
    {
        free(pool_mgr->node_heap);
        free(&pool_mgr[0].pool);
        free(pool_mgr);
        return NULL;

    }
    // assign all the pointers and update meta data:
    //   initialize top node of node heap
    pool_mgr->node_heap[0].next = NULL;
    pool_mgr->node_heap[0].prev = NULL;
    pool_mgr->node_heap[0].allocated = 0;
    pool_mgr->node_heap[0].used = 0;
    pool_mgr->node_heap[0].alloc_record.mem = pool_mgr->pool.mem;
    pool_mgr->node_heap[0].alloc_record.size = size;

    //   initialize top node of gap index
    pool_mgr->gap_ix[0].size = size;
    pool_mgr->gap_ix[0].node = pool_mgr->node_heap;

    //   initialize pool mgr
    pool_mgr->pool.alloc_size = 0;
    pool_mgr->pool.num_allocs = 0;
    pool_mgr->pool.total_size = size;
    pool_mgr->pool.num_gaps = 1;
    pool_mgr->pool.policy = policy;
    pool_mgr->used_nodes = 1;

    //   link pool mgr to pool store
    int i = 0;
    while (pool_store[i] != NULL)
        ++i;
    pool_store[i] = pool_mgr;
    pool_store_size = 1;

    // return the address of the mgr, cast to (pool_pt)
    return (pool_pt) pool_mgr;
}

alloc_status mem_pool_close(pool_pt pool)
{
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt)pool;

    // check if this pool is allocated
    if (pool_mgr == NULL)
        return ALLOC_NOT_FREED;

    // check if pool has only one gap
    if (pool->num_gaps != 1)
        return ALLOC_NOT_FREED;

    // check if it has zero allocations
    if (pool->num_allocs != 0)
        return ALLOC_NOT_FREED;

    // free memory pool
    free(pool);

    // free node heap
    free(pool_mgr->node_heap);

    // free gap index
    free(pool_mgr->gap_ix);

    // find mgr in pool store and set to null
    int i = 0;
    while (pool_store[i] != pool_mgr)
        ++i;
    pool_store[i] = NULL;

    // free mgr
    //free(pool_mgr);

    return ALLOC_OK;

}

alloc_pt mem_new_alloc(pool_pt pool, size_t size)
{
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;

    // check if any gaps, return null if none
    if (pool_mgr->gap_ix == NULL)
        return NULL;

    // expand heap node, if necessary, quit on error
    if (pool_mgr->used_nodes / pool_mgr->total_nodes > MEM_NODE_HEAP_FILL_FACTOR)
        if (_mem_resize_node_heap(pool_mgr) != ALLOC_OK)
            return NULL;

    // check used nodes fewer than total nodes, quit on error
    if (pool_mgr->total_nodes > pool_mgr->used_nodes)
        return NULL;

    // get a node for allocation:
    node_pt new_node;
    int i = 0;

    // if FIRST_FIT, then find the first sufficient node in the node heap
    if(pool_mgr->pool.policy == FIRST_FIT)
    {
        node_pt heap = pool_mgr->node_heap;
        while(pool_mgr->node_heap[i].allocated != 0 && pool_mgr->node_heap[i].used !=1)
            ++i;

        if(heap == NULL)
            return NULL;

        new_node = &pool_mgr->node_heap[i];
    }

        // if BEST_FIT, then find the first sufficient node in the gap index
    else if(pool_mgr->pool.policy == BEST_FIT)
    {
        if (pool_mgr->pool.num_gaps > 0) {
            while (i < pool_mgr->pool.num_gaps && pool_mgr->gap_ix[i + 1].size >= size)
                ++i;
        }

        else
            return NULL;

        new_node = pool_mgr->gap_ix[i].node;
    }

    // check if node found
    if(new_node == NULL)
        return NULL;

    // update metadata (num_allocs, alloc_size)
    pool_mgr->pool.alloc_size += size;
    ++pool_mgr->pool.num_allocs;

    // calculate the size of the remaining gap, if any
    size_t remainder = 0;
    if (new_node->alloc_record.size - size > 0)
        remainder = new_node->alloc_record.size - size;

    // remove node from gap index
    _mem_remove_from_gap_ix(pool_mgr,size, new_node);

    // convert gap_node to an allocation node of given size
    new_node->allocated = 1;
    new_node->used = 1;
    new_node->alloc_record.size = size;

    // adjust node heap:
    if (remainder != 0)
    {
        //if remaining gap, need a new node
        //find an unused one in the node heap
        int i = 0;
        while (pool_mgr->node_heap[i].used != 0 && pool_mgr->node_heap[i].allocated != 0)
            ++i;
        node_pt new_gap = &pool_mgr->node_heap[i];

        //initialize it to a gap node
        if (new_gap == NULL) {
            new_gap->used = 1;
            new_gap->allocated = 1;
            new_gap->alloc_record.size = remainder;
        }

        //update metadata (used_nodes)
        ++pool_mgr->used_nodes;

        //update linked list (new node right after the node for allocation)
        new_node->alloc_record.size = size;
        new_node->next = new_gap;
        new_gap->prev = new_node;

        //add to gap index
        _mem_add_to_gap_ix(pool_mgr, size, new_gap);
    }

    // return allocation record by casting the node to (alloc_pt)
    return (alloc_pt)new_node;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc)
{
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    // get node from alloc by casting the pointer to (node_pt)
    // find the node in the node heap
    // this is node-to-delete
    // make sure it's found
    // convert to gap node
    // update metadata (num_allocs, alloc_size)
    // if the next node in the list is also a gap, merge into node-to-delete
    //   remove the next node from gap index
    //   check success
    //   add the size to the node-to-delete
    //   update node as unused
    //   update metadata (used nodes)
    //   update linked list:
    /*
                    if (next->next) {
                        next->next->prev = node_to_del;
                        node_to_del->next = next->next;
                    } else {
                        node_to_del->next = NULL;
                    }
                    next->next = NULL;
                    next->prev = NULL;
     */

    // this merged node-to-delete might need to be added to the gap index
    // but one more thing to check...
    // if the previous node in the list is also a gap, merge into previous!
    //   remove the previous node from gap index
    //   check success
    //   add the size of node-to-delete to the previous
    //   update node-to-delete as unused
    //   update metadata (used_nodes)
    //   update linked list
    /*
                    if (node_to_del->next) {
                        prev->next = node_to_del->next;
                        node_to_del->next->prev = prev;
                    } else {
                        prev->next = NULL;
                    }
                    node_to_del->next = NULL;
                    node_to_del->prev = NULL;
     */
    //   change the node to add to the previous node!
    // add the resulting node to the gap index
    // check success

    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;

    // get node from alloc by casting the pointer to (node_pt)
    node_pt node = (node_pt) alloc;
    node_pt deletion = NULL;

    // find the node in the node heap
    for (int i=0; i < pool_mgr->total_nodes; ++i)
    {
        if(node == &pool_mgr->node_heap[i])
        {
            deletion = &pool_mgr->node_heap[i];
            break;
        }
    }

    // this is node-to-delete
    // make sure it's found
    if (deletion == NULL)
        return ALLOC_FAIL;

    // update metadata (num_allocs, alloc_size)
    deletion->allocated = 0;
    --pool_mgr->pool.num_allocs;
    pool_mgr->pool.alloc_size = pool_mgr->pool.alloc_size - deletion->alloc_record.size;

    // if the next node in the list is also a gap, merge into node-to-delete
    if (deletion->next != NULL && deletion->next->allocated == 0)
    {
        node_pt next = deletion->next;
        if (_mem_remove_from_gap_ix(pool_mgr, 0, next) == ALLOC_FAIL)
            return ALLOC_FAIL;

        deletion->alloc_record.size += next->alloc_record.size;
        next->used = 0;
        --pool_mgr->used_nodes;

        if (next->next)
        {
            next->next->prev = deletion;
            deletion->next = next->next;
        }

        else
            deletion->next = NULL;

        next->next = NULL;
        next->prev = NULL;
    }

    // this merged node-to-delete might need to be added to the gap index
    // but one more thing to check...
    // if the previous node in the list is also a gap, merge into previous!
    if(deletion->prev != NULL && deletion->prev->allocated == 0)
    {
        node_pt previous = deletion->prev;
        if (_mem_remove_from_gap_ix(pool_mgr, 0, previous) == ALLOC_FAIL)
            return ALLOC_FAIL;

        previous->alloc_record.size = previous->alloc_record.size - deletion->alloc_record.size;

        deletion->used = 0;
        --pool_mgr->used_nodes;
        if (deletion->next)
        {
            previous->next = deletion->next;
            deletion->next->next->prev = previous;
        }
        else
            previous->next = NULL;

        deletion->next = NULL;
        deletion->prev = NULL;
    }

    // add the resulting node to the gap index
    // check success
    if (_mem_add_to_gap_ix(pool_mgr, deletion->alloc_record.size, deletion) != ALLOC_OK)
        return ALLOC_FAIL;
    else
        return ALLOC_OK;
}

void mem_inspect_pool(pool_pt pool, pool_segment_pt *segments, unsigned *num_segments)
{
    // get the mgr from the pool
    // allocate the segments array with size == used_nodes
    // check successful
    // loop through the node heap and the segments array
    //    for each node, write the size and allocated in the segment
    // "return" the values:
    /*
                    *segments = segs;
                    *num_segments = pool_mgr->used_nodes;
     */

    // get the mgr from the pool
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;

    // allocate the segments array with size == used_nodes
    pool_segment_pt segmentArr = (pool_segment_pt) calloc(pool_mgr->used_nodes, sizeof(pool_segment_t));
    assert(segmentArr);
    node_pt current = pool_mgr->node_heap;

    // loop through the node heap and the segments array
    //    for each node, write the size and allocated in the segment
    for (int i=0; i < pool_mgr->used_nodes; ++i)
    {
        segmentArr[i].size = current->alloc_record.size;
        segmentArr[i].allocated = current->allocated;
        if (current->next != NULL)
            current = current->next;
    }

    // "return" the values:
    *segments = segmentArr;
    *num_segments = pool_mgr->used_nodes;
}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store()
{
    if (((float) pool_store_size / pool_store_capacity) > MEM_POOL_STORE_FILL_FACTOR) {
        pool_store = realloc(pool_store, sizeof(pool_store) * MEM_POOL_STORE_EXPAND_FACTOR);
        pool_store_capacity = pool_store_capacity * MEM_POOL_STORE_EXPAND_FACTOR;

        return ALLOC_OK;
    }
    else {
        return ALLOC_FAIL;
    }
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr)
{
    if (pool_mgr->used_nodes / pool_mgr->total_nodes > MEM_NODE_HEAP_FILL_FACTOR)
    {
        pool_mgr->node_heap = realloc(pool_mgr->node_heap, sizeof(pool_mgr->node_heap) * MEM_NODE_HEAP_EXPAND_FACTOR);
        pool_mgr->total_nodes = pool_mgr->total_nodes * MEM_NODE_HEAP_EXPAND_FACTOR;

        return ALLOC_OK;
    }

    else
    {
        return ALLOC_FAIL;
    }
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr)
{
    if (pool_mgr->gap_ix->size / pool_mgr->gap_ix_capacity > MEM_GAP_IX_FILL_FACTOR)
    {
        pool_mgr->gap_ix = realloc(pool_mgr->gap_ix, sizeof(pool_mgr->gap_ix) * MEM_GAP_IX_EXPAND_FACTOR);
        pool_mgr->gap_ix_capacity = pool_mgr->gap_ix_capacity * MEM_GAP_IX_EXPAND_FACTOR;

        return ALLOC_OK;
    }

    else
    {
        return ALLOC_FAIL;
    }
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr, size_t size, node_pt node)
{
    // expand the gap index, if necessary (call the function)
    _mem_resize_gap_ix(pool_mgr);

    // add the entry at the end
    int i = 0;
    while (pool_mgr->gap_ix[i].node != NULL)
        ++i;

    pool_mgr->gap_ix[i].node = node;
    pool_mgr->gap_ix[i].size = size;

    // update metadata (num_gaps)
    ++pool_mgr->pool.num_gaps;

    // sort the gap index (call the function)
    // check success
    if (pool_mgr->gap_ix[i].node != NULL)
    {
        _mem_sort_gap_ix(pool_mgr);
        return ALLOC_OK;
    }

    else
        return ALLOC_FAIL;
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr, size_t size, node_pt node)
{
    int i = 0;
    while (pool_mgr->gap_ix[i].node != node)
        ++i;

    pool_mgr->gap_ix[i].size = 0;
    pool_mgr->gap_ix[i].node = NULL;

    return ALLOC_OK;
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr)
{
    int i = pool_mgr->pool.num_gaps - 1;

    while (pool_mgr->gap_ix[i].size < pool_mgr->gap_ix[i+1].size && i > 0)
    {
        gap_t temp = pool_mgr->gap_ix[i];
        pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i+1];
        pool_mgr->gap_ix[i+1] = temp;

        --i;
    }

    return ALLOC_OK;
}

