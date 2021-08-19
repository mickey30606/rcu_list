/* A concurrent linked list utilizing the simplified RCU algorithm */
#include <stdbool.h>

typedef struct rcu_list rcu_list_t;

typedef struct {
  struct list_node *entry;
} iterator_t;

typedef struct {
  struct rcu_list *list;
  struct zombie_node *zombie;
} rcu_handle_t;

typedef rcu_handle_t read_handle_t;
typedef rcu_handle_t write_handle_t;

typedef void (*deleter_func_t)(void *);
typedef bool (*finder_func_t)(void *, void *);

#define _GNU_SOURCE
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <threads.h>

typedef struct list_node {
  bool deleted;
  struct list_node *next, *prev;
  void *data;
} list_node_t;

typedef struct zombie_node {
  struct zombie_node *next;
  struct list_node *zombie;
  rcu_handle_t *owner;
} zombie_node_t;

struct rcu_list {
  pthread_mutex_t write_lock; /* exclusive lock acquired by writers */
  list_node_t *head, *tail;   /* head and tail of the "live" list */
  zombie_node_t *zombie_head; /* head of the zombie list */
  deleter_func_t deleter;
};

#ifdef ANALYZE

#define MAX_THREAD 50

enum { READ_THREAD = 0, WRITE_PUSH, WRITE_DELETE };

typedef struct analyze_node {
  int type;
  int list_long;
  list_node_t *curr_read_head;
  zombie_node_t *zombie;
} analyze_node_t;

typedef struct zombie_sequence {
  zombie_node_t *order[MAX_THREAD];
  int size;
  zombie_node_t *buffer[MAX_THREAD];
  int buffer_size;
} zombie_sequence_t;

static thread_local int tid_v = 0;
static int tid_v_base = 0;

static analyze_node_t zombie_data[MAX_THREAD + 1] = {0};
static zombie_sequence_t sequence = {0};

#define GET_TID                                                                \
  ({                                                                           \
    if (!tid_v) {                                                              \
      tid_v = __atomic_add_fetch(&tid_v_base, 1, __ATOMIC_RELAXED);            \
    }                                                                          \
    tid_v;                                                                     \
  })

#define TRACE_LISTHEAD_AND_LENGTH(current_head)                                \
  ({                                                                           \
    zombie_data[GET_TID].curr_read_head = current_head;                        \
    list_node_t *tmp;                                                          \
    int count = 0;                                                             \
    list_for_each_entry(tmp, zombie_data[GET_TID].curr_read_head, next) {      \
      count++;                                                                 \
    }                                                                          \
    zombie_data[GET_TID].list_long = count;                                    \
  })

#define CONNECT_ZOMBIE(zombie_node)                                            \
  ({ zombie_data[GET_TID].zombie = zombie_node; })

#define INPUT_SEQ_BUFFER(zombie)                                               \
  ({ sequence.buffer[sequence.buffer_size++] = zombie; })

#define FLUSH_SEQ_BUFFER                                                       \
  ({                                                                           \
    for (int i = sequence.buffer_size - 1; i >= 0; i--) {                      \
      sequence.order[sequence.size++] = sequence.buffer[i];                    \
    }                                                                          \
    sequence.buffer_size = 0;                                                  \
  })

#define DISTINGUISH_TYPE(iter)                                                 \
  ({                                                                           \
    if (iter) {                                                                \
      zombie_data[GET_TID].type = WRITE_DELETE;                                \
    } else {                                                                   \
      zombie_data[GET_TID].type = WRITE_PUSH;                                  \
    }                                                                          \
  })

#define ANALYZE_SHOW(list)                                                     \
  ({                                                                           \
    zombie_node_t *tmp,                                                        \
        *head = __atomic_load_n(&list->zombie_head, __ATOMIC_RELAXED);         \
    list_for_each_entry(tmp, head, next) { INPUT_SEQ_BUFFER(tmp); }            \
    FLUSH_SEQ_BUFFER;                                                          \
    for (int i = 0; i < sequence.size; i++) {                                  \
      for (int j = 1; j <= sequence.size; j++) {                               \
        if (sequence.order[i] == zombie_data[j].zombie) {                      \
          printf("---------------------\n");                                   \
          switch (zombie_data[j].type) {                                       \
          case READ_THREAD:                                                    \
            printf("| %16s  |\n", "read");                                     \
            break;                                                             \
          case WRITE_PUSH:                                                     \
            printf("| %16s  |\n", "push");                                     \
            break;                                                             \
          case WRITE_DELETE:                                                   \
            printf("| %16s  |\n", "delete");                                   \
            break;                                                             \
          }                                                                    \
          if (zombie_data[j].list_long) {                                      \
            printf("| long = %9d  |\n", zombie_data[j].list_long);             \
          }                                                                    \
          if (zombie_data[j].curr_read_head) {                                 \
            printf("|head=%p|\n", zombie_data[j].curr_read_head);              \
          }                                                                    \
          printf("---------------------\n");                                   \
          printf("        \\|/         \n");                                   \
          break;                                                               \
        }                                                                      \
        if (j == sequence.size) {                                              \
          printf("node may error!!! with %p\n", sequence.order[i]);            \
        }                                                                      \
      }                                                                        \
    }                                                                          \
  })

#else

#define GET_TID
#define TRACE_LISTHEAD_AND_LENGTH
#define CONNECT_ZOMBIE(zombie_node)
#define INPUT_SEQ_BUFFER(zombie)
#define FLUSH_SEQ_BUFFER
#define DISTINGUISH_TYPE(iter)
#define ANALYZE_SHOW

#endif

static list_node_t *make_node(void *data);

static zombie_node_t *make_zombie_node(void);

static void lock_for_write(rcu_list_t *list);
static void unlock_for_write(rcu_list_t *list);

/* use in unlock */
#define list_for_each_entry(node, head, next)                                  \
  for (node = head; node; __atomic_load(&node->next, &node, __ATOMIC_SEQ_CST))

static rcu_list_t *list_new_with_deleter(deleter_func_t deleter) {
  if (!deleter)
    return NULL;

  rcu_list_t *list = malloc(sizeof(rcu_list_t));
  if (!list)
    return NULL;

  if (pthread_mutex_init(&list->write_lock, NULL) != 0) {
    free(list);
    return NULL;
  }

  list->head = list->tail = NULL;
  list->zombie_head = NULL;
  list->deleter = deleter;

  return list;
}

/* what about the zombies? */
static void list_free(void *arg) {
  rcu_list_t *list = arg;
  for (list_node_t *iter = list->head; iter;) {
    list_node_t *tmp = iter->next;
    free(iter->data);
    free(iter);
    iter = tmp;
  }
  free(list);
}

rcu_list_t *list_new(void) { return list_new_with_deleter(list_free); }

void list_delete(rcu_list_t *list) {
  if (!list || !list->deleter)
    return;
  ANALYZE_SHOW(list);
  list->deleter(list);
}

/**/
void list_push_front(rcu_list_t *list, void *data, write_handle_t *handle) {
  if (!list)
    return;

  list_node_t *node = make_node(data);
  if (!node)
    return;

  list_node_t *old_head;
  __atomic_load(&list->head, &old_head, __ATOMIC_RELAXED);

  if (!old_head) {
    /* list is currently empty */
    __atomic_store(&list->head, &node, __ATOMIC_SEQ_CST);
    __atomic_store(&list->tail, &node, __ATOMIC_SEQ_CST);
  } else {
    /* general case */
    __atomic_store(&node->next, &old_head, __ATOMIC_SEQ_CST);
    __atomic_store(&old_head->prev, &node, __ATOMIC_SEQ_CST);
    __atomic_store(&list->head, &node, __ATOMIC_SEQ_CST);
  }

  //    unlock_for_write(list);
}

/**/
void list_delete_node(rcu_list_t *list, write_handle_t *handle,
                      iterator_t *iter) {
  if (!iter->entry)
    return;
  if (!iter->entry->prev && !iter->entry->next) {
    __atomic_store_n(&list->head, NULL, __ATOMIC_SEQ_CST);
  } else if (!iter->entry->prev) {
    __atomic_store(&list->head, &iter->entry->next, __ATOMIC_SEQ_CST);
    __atomic_store_n(&iter->entry->next->prev, NULL, __ATOMIC_SEQ_CST);
  } else if (!iter->entry->next) {
    __atomic_store_n(&iter->entry->prev->next, NULL, __ATOMIC_SEQ_CST);
  } else {
    __atomic_store(&iter->entry->prev->next, &iter->entry->next,
                   __ATOMIC_RELEASE);
    __atomic_store(&iter->entry->next->prev, &iter->entry->prev,
                   __ATOMIC_RELEASE);
  }
}

iterator_t list_find(rcu_list_t *list, void *data, finder_func_t finder) {
  iterator_t iter = {.entry = NULL}; /* initialize an invalid iterator */
  if (!list)
    return iter;

  list_node_t *current;
  __atomic_load(&list->head, &current, __ATOMIC_SEQ_CST);

  TRACE_LISTHEAD_AND_LENGTH(current);

  while (current) {
    if (finder(current->data, data)) {
      iter.entry = current;
      break;
    }

    __atomic_load(&current->next, &current, __ATOMIC_SEQ_CST);
  }


  return iter;
}

iterator_t list_begin(rcu_list_t *list, read_handle_t *handle) {
  iterator_t iter = {.entry = NULL};
  if (!list)
    return iter;

  list_node_t *head;
  __atomic_load(&list->head, &head, __ATOMIC_SEQ_CST);

  iter.entry = head;
  return iter;
}

void *iterator_get(iterator_t *iter) {
  return (iter && iter->entry) ? iter->entry->data : NULL;
}

read_handle_t list_register_reader(rcu_list_t *list) {
  read_handle_t handle = {.list = list, .zombie = NULL};
  return handle;
}

write_handle_t list_register_writer(rcu_list_t *list) {
  write_handle_t handle = {.list = list, .zombie = NULL};
  return handle;
}

void rcu_read_lock(read_handle_t *handle) {
  zombie_node_t *z_node = make_zombie_node();
  CONNECT_ZOMBIE(z_node);

  z_node->owner = handle;
  handle->zombie = z_node;

  rcu_list_t *list = handle->list;

  zombie_node_t *old_head;
  __atomic_load(&list->zombie_head, &old_head, __ATOMIC_SEQ_CST);

  /* what if list->zombie_head change? */
  do {
    __atomic_store(&z_node->next, &old_head, __ATOMIC_SEQ_CST);

  } while (!__atomic_compare_exchange(&list->zombie_head, &old_head, &z_node,
                                      true, __ATOMIC_SEQ_CST,
                                      __ATOMIC_SEQ_CST));
}

void rcu_read_unlock(read_handle_t *handle) {
  zombie_node_t *z_node = handle->zombie;

  zombie_node_t *cached_next;
  __atomic_load(&z_node->next, &cached_next, __ATOMIC_SEQ_CST);

  bool last = true;

  /* walk through the zombie list to determine if this is the last active
   * reader in the list.
   */
  zombie_node_t *n = cached_next;
  while (n) {
    list_node_t *owner;
    __atomic_load(&n->owner, &owner, __ATOMIC_SEQ_CST);

    if (owner) {
      last = false; /* this is not the last active reader */
      break;
    }

    __atomic_load(&n->next, &n, __ATOMIC_SEQ_CST);
  }

  n = cached_next;

  if (last) {
    while (n) {
      list_node_t *dead_node = n->zombie;
      free(dead_node);

      zombie_node_t *old_node = n;
      __atomic_load(&n->next, &n, __ATOMIC_SEQ_CST);

      INPUT_SEQ_BUFFER(old_node);

      free(old_node);
    }

    __atomic_store(&z_node->next, &n, __ATOMIC_SEQ_CST);
    FLUSH_SEQ_BUFFER;
  }

  void *null = NULL;
  __atomic_store(&z_node->owner, &null, __ATOMIC_SEQ_CST);
}

void rcu_write_lock(write_handle_t *handle) { lock_for_write(handle->list); }

void rcu_write_unlock(write_handle_t *handle, iterator_t *iter) {
  rcu_read_lock(handle);

  DISTINGUISH_TYPE(iter);

  if (iter && iter->entry)
    __atomic_store(&handle->zombie->zombie, &iter->entry, __ATOMIC_SEQ_CST);
  rcu_read_unlock(handle);
  unlock_for_write(handle->list);
}

static list_node_t *make_node(void *data) {
  list_node_t *node = malloc(sizeof(list_node_t));
  if (!node)
    return NULL;

  node->data = data;
  node->next = node->prev = NULL;
  node->deleted = false;

  return node;
}

static zombie_node_t *make_zombie_node(void) {
  zombie_node_t *z_node = malloc(sizeof(zombie_node_t));
  if (!z_node)
    return NULL;

  z_node->zombie = NULL;
  z_node->owner = NULL;
  z_node->next = NULL;

  return z_node;
}

static void lock_for_write(rcu_list_t *list) {
  pthread_mutex_lock(&list->write_lock);
}

static void unlock_for_write(rcu_list_t *list) {
  pthread_mutex_unlock(&list->write_lock);
}

/* test program starts here */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <threads.h>

static thread_local int insert_v = 0;
static int insert_base = 0;

static thread_local int delete_v = 0;
static int delete_base = 0;

pthread_barrier_t bar;

static inline int insert(void){
    if(insert_v == 0){
        insert_v = __atomic_add_fetch(&insert_base, 1, __ATOMIC_SEQ_CST);
    }
    return insert_v;
}

static inline int delete(void){
    if(delete_v == 0){
        delete_v = __atomic_add_fetch(&delete_base, 1, __ATOMIC_SEQ_CST);
    }
    return delete_v;
}

typedef struct dummy {
    int a, b;
} dummy_t;

static dummy_t *make_dummy(int a, int b)
{
    dummy_t *dummy = malloc(sizeof(dummy_t));
    dummy->a = a, dummy->b = b;
    return dummy;
}

static bool finder(void *x, void *y)
{
    dummy_t *dx = x, *dy = y;
    return (dx->a == dy->a) && (dx->b == dy->b);
}

static void *reader_thread(void *arg)
{
    pthread_barrier_wait(&bar);
    rcu_list_t *list = arg;
    read_handle_t reader = list_register_reader(list);

    rcu_read_lock(&reader);

    /* read from list here */
//    iterator_t iter = list_find(list, &(dummy_t){1, 1}, finder);
    list_find(list, &(dummy_t){1, 1}, finder);

    rcu_read_unlock(&reader);
    return NULL;
}

static void *writer_thread_push_front(void *arg)
{
    pthread_barrier_wait(&bar);
    dummy_t *d1 = make_dummy(1, 1);
//    dummy_t *d1 = make_dummy(insert(), insert());

    rcu_list_t *list = arg;
    write_handle_t writer = list_register_writer(list);

    rcu_write_lock(&writer);

    /* write to list here */
    list_push_front(list, d1, &writer);
    rcu_write_unlock(&writer, NULL);

    /*
    dummy_t *d2 = make_dummy(2, 2);
    list_push_front(list, d2, &writer);

    rcu_write_unlock(&writer, NULL);

    rcu_write_lock(&writer);

    iterator_t iter = list_find(list, d1, finder);

    list_delete_node(&iter);

    rcu_write_unlock(&writer, &iter);
    */
    return NULL;
}

static void *writer_thread_delete(void *arg){
    pthread_barrier_wait(&bar);
//    dummy_t *d1 = make_dummy(delete(), delete());
    dummy_t *d1 = make_dummy(2, 2);

    rcu_list_t *list = arg;
    write_handle_t writer = list_register_writer(list);

    rcu_write_lock(&writer);

    iterator_t iter = list_find(list, d1, finder);
    list_delete_node(list, &writer, &iter);

    rcu_write_unlock(&writer, &iter);

    return NULL;
}

#define N_READERS 20
#define N_WRITERS 10

int main(void)
{
    pthread_barrier_init(&bar, NULL, N_READERS+N_WRITERS);
    rcu_list_t *list = list_new();
    for(int i=0;i<10;i++){

        dummy_t *d0 = make_dummy(2, 2);
        list_push_front(list, d0, NULL);
    }

    pthread_t t0[N_WRITERS], t_r[N_READERS];
//    pthread_create(&t0, NULL, writer_thread, list);
    for (int i = 0; i < N_READERS; i++)
        pthread_create(&t_r[i], NULL, reader_thread, list);
    for(int i = 0;i< N_WRITERS;i++){
        pthread_create(&t0[i], NULL,
                (i&1)?(writer_thread_push_front):(writer_thread_delete),
                list);
    }

    for (int i = 0; i < N_READERS; i++){
        pthread_join(t_r[i], NULL);
    }
    for(int i = 0;i<N_WRITERS;i++){
        pthread_join(t0[i], NULL);
    }

    pthread_barrier_destroy(&bar);

    list_delete(list);

    return EXIT_SUCCESS;
}
