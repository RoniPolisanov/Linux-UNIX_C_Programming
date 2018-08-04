/******************************************************************************
 * * FILE: threadPool.c
 * * DESCRIPTION:
 * *   The system will be composed of three main components:
 * *   Thread Pool Manager - Responsible of distributing tasks amongst its available threads.
 * *   Task Feeder/s - Responsible of loading new tasks into the Thread Pool Manager.
 * *   Main application - Responsible for approximating PI using the Monte-Carlo method.
 * *
 * * HOW TO COMPILE:
 * *   gcc -lm -pthread threadPool.c
 * *
 * * HOW TO RUN:
 * *    ./threadPool arg1 arg2 arg3 arg4
 * *
 * * AUTHOR: Roni Polisanov
 * * A third year software engineering student at Shenkar College - High School of Engineering.
 *******************************************************************************/
 
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <pthread.h>
#include <math.h>
#include <errno.h>
#include <time.h>

#define MB10 10000000

extern pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
extern pthread_cond_t  cond  =	PTHREAD_COND_INITIALIZER;

//"FLAGs" for "doTask" function ::: lock\unlock.
static volatile int threads_keepalive;
static volatile int threads_on_hold;

FILE* fp;

//taskRetData Contains stored data in thread pool.
typedef struct taskRetData_t {
    int size;
    char buffer[4];
} taskRetData_t;

//Thread Contains thread id count and result.
typedef struct thread_t {
    int id;
    int count;
    pthread_t threadID;
    struct taskRetData_t** results;
    struct tp_manager_t* myManager;
} thread_t;

//Task Contains monteCarlo function with its argument.
typedef struct task_t {
    void* arg;
    void* (*func)(void*);
    struct task_t* prev;
} task_t;

//Task Feeder Struct (Queue)
typedef struct task_feeder_t {
    int len;
    long maxThreads;    //T argument - Allocating max number of threads per task.
    struct task_t* front;
    struct task_t* rear;
} task_feeder_t;

//tp_manager_t Contains most of the information.
typedef struct tp_manager_t {
    unsigned long N, F, T, num_threads_working, num_threads_alive;
    long memOffset;
    char dataPool[MB10];
    struct thread_t** threadPool;
    struct task_feeder_t taskFeeder;
} tp_manager_t;

//Extracting argument to the monte-carlo function
long getArg(struct thread_t* thread, task_t* task_p)
{
    struct thread_t* threadTemp = thread;
    long* temp = &task_p->arg;
    long updateArg = *temp;
    updateArg = updateArg/threadTemp->myManager->N;
    if ((threadTemp)->id == 0) {
        updateArg += (long)task_p->arg% threadTemp->myManager->N;
        return updateArg;
    }
    return updateArg;
}

//Log writing to file
void Log(const char *data)
{
    if (fp != NULL) {
        fprintf(fp, data);
    }
    return NULL;
}

//return the pointer to file
FILE* getFPS()
{
    return fp;
}

//Initialize Task Feeder Queue
static int taskFeeder_init(task_feeder_t* taskFeeder_p)
{
    taskFeeder_p->len = 0;
    taskFeeder_p->front = NULL;
    taskFeeder_p->rear  = NULL;

    return 0;
}

//Add Task to our Task Feeder Queue
static void taskFeeder_push(struct task_feeder_t* taskFeeder_p, struct task_t* newtask, long F, long T)
{
    pthread_mutex_lock(&mutex);
    newtask->prev = NULL;
    printf("Entering to taskFeeder_push\n");
    Log("Entering to taskFeeder_push\n");

    taskFeeder_p->maxThreads = T;
    printf("ThreadPoolMAnager Allocating %ld threads that can work per task.\n", T);
    fprintf(getFPS(), "ThreadPoolMAnager Allocating %ld threads that can work per task.\n", T);

    //Checking Maximum number of tasks
    if(taskFeeder_p->len >= F) {
        perror("ERROR: You have reached your maximum tasks capacity.\n");
        Log("ERROR: You have reached your maximum tasks capacity.\n");
        return;
    }
    switch(taskFeeder_p->len) {
    case 0:  /* if no tasks in queue */
        taskFeeder_p->front = newtask;
        taskFeeder_p->rear  = newtask;
        break;

    default: /* if tasks in queue */
        taskFeeder_p->rear->prev = newtask;
        taskFeeder_p->rear = newtask;
    }
    taskFeeder_p->len++;

    pthread_mutex_unlock(&mutex);
}

//Poping task from our Task Feeder Queue
struct task_t* taskFeeder_pull(struct task_feeder_t* taskFeeder_p)
{
    pthread_mutex_lock(&mutex);
    struct task_t* task_p = taskFeeder_p->front;

    switch(taskFeeder_p->len) {

    //if no tasks in queue
    case 0:
        break;

    //if one task in queue
    case 1:
        taskFeeder_p->front = NULL;
        taskFeeder_p->rear  = NULL;
        taskFeeder_p->len = 0;
        printf("LAST Pop in taskFeeder\n");
        Log("LAST Pop in taskFeeder\n");
        break;

    //if>1 tasks in queue
    default:
        printf("Pop in taskFeeder\n");
        Log("Pop in taskFeeder\n");
        taskFeeder_p->front = task_p->prev;
        taskFeeder_p->len--;
    }

    pthread_mutex_unlock(&mutex);
    return task_p;
}

//Clear the Task Feeder Queue
static void taskFeeder_destroy(task_feeder_t* taskFeeder_p)
{
    while(taskFeeder_p->len) {
        free(taskFeeder_pull(taskFeeder_p));
    }

    taskFeeder_p->front = NULL;
    taskFeeder_p->rear  = NULL;
    taskFeeder_p->len = 0;
}

//thread "target" function
static void* doTask(struct thread_t *thread_p)
{
    int i;
    long *Argument;
    printf("Entering In doTask\n");
    Log("Entering In doTask\n");
    tp_manager_t* tpManager = thread_p->myManager;
    //Mark thread as alive (initialized)
    pthread_mutex_lock(&mutex);
    tpManager->num_threads_alive++;
    printf("doTask ::: num_threads_alive %d\n", tpManager->num_threads_alive);
    fprintf(getFPS(), "doTask ::: num_threads_alive %d\n", tpManager->num_threads_alive);
    //getchar();
    pthread_mutex_unlock(&mutex);
    
    //Alternative to while loop
    if(threads_keepalive) {
        printf("doTask ::: Thread ID Before wait: %d\n", pthread_self());
        fprintf(getFPS(), "doTask ::: Thread ID Before wait: %d\n", pthread_self());
        pthread_mutex_lock(&mutex);
        pthread_cond_wait(&cond,&mutex);
        printf("doTask ::: Thread ID In wait: %d\n", pthread_self());
        fprintf(getFPS(), "doTask ::: Thread ID In wait: %d\n", pthread_self());
        pthread_mutex_unlock(&mutex);
        printf("doTask ::: Thread ID After wait: %d\n", pthread_self());
        fprintf(getFPS(), "doTask ::: Thread ID After wait: %d\n", pthread_self());

        if (threads_keepalive) {
            pthread_mutex_lock(&mutex);
            ++tpManager->num_threads_working;
            pthread_mutex_unlock(&mutex);
            printf("doTask ::: num_threads_working: %d\n", tpManager->num_threads_working);
            fprintf(getFPS(), "doTask ::: num_threads_working: %d\n", tpManager->num_threads_working);
            printf("doTask ::: Thread ID working now: %d\n", pthread_self());
            fprintf(getFPS(), "doTask ::: Thread ID working now: %d\n", pthread_self());

            struct task_t* task_p = taskFeeder_pull(&tpManager->taskFeeder);
            //Read task from queue and execute it
            if (task_p) {
                Argument = getArg(thread_p, task_p);
                sprintf(thread_p->results[(thread_p)->count]->buffer, "%f",*(float*)task_p->func(&Argument));
                printf("doTask ::: Number of iterations for Monte-Carlo: %s\n",thread_p->results[thread_p->count]->buffer);
                fprintf(getFPS(), "doTask ::: Number of iterations for Monte-Carlo: %s\n",thread_p->results[thread_p->count]->buffer);
                (thread_p->count)++;
            }

            pthread_mutex_lock(&mutex);
            tpManager->num_threads_working--;
            printf("doTask ::: Updated number of threads working after decreasing: %d\n", tpManager->num_threads_working);
            fprintf(getFPS(), "doTask ::: Updated number of threads working after decreasing: %d\n", tpManager->num_threads_working);

            if (!tpManager->num_threads_working) {
                printf("doTask ::: Now Signaling for the next thread!!!", tpManager->num_threads_working);
                fprintf(getFPS(), "doTask ::: Now Signaling for the next thread!!!", tpManager->num_threads_working);
                pthread_cond_signal(&cond);
            }
            pthread_mutex_unlock(&mutex);
        }
    }

    pthread_mutex_lock(&mutex);
    --tpManager->num_threads_alive;

    pthread_mutex_unlock(&mutex);
    pthread_exit((void*)thread_p);
}

//Creating the thread pool manager
tp_manager_t* create_manager(long N, long T, long F)
{
    threads_keepalive = 1;

    tp_manager_t* newManager = (struct tp_manager_t*)malloc(sizeof(struct tp_manager_t));
    if(newManager == NULL) {
        perror("Cannot allocate memory for create manager.\n");
        Log("Cannot allocate memory for create manager.\n");
        return NULL;
    }

    newManager->N = N;
    newManager->T = T;
    newManager->F = F;

    //Initializing the taskFeeder
    if (taskFeeder_init(&newManager->taskFeeder) == -1) {
        perror("taskFeeder_init(): Could not allocate memory for job queue\n");
        Log("taskFeeder_init(): Could not allocate memory for job queue\n");
        free(newManager);
        return NULL;
    }
    printf("taskFeeder Initialized Successfully.\n");
    Log("taskFeeder Initialized Successfully.\n");

    //Make threads in pool
    newManager->threadPool = (struct thread_t**)malloc(N * sizeof(struct thread_t *));
    if (newManager->threadPool == NULL) {
        perror("thpool_init(): Could not allocate memory for threads\n");
        Log("thpool_init(): Could not allocate memory for threads\n");
        taskFeeder_destroy(&newManager->taskFeeder);
        free(newManager);
        return NULL;
    }

    int n;
    for (n = 0; n < N; n++) {
        if(threadPool_init(newManager, &newManager->threadPool[n], n) == 0) {
            printf("Created thread %d in pool \n", n);
            fprintf(getFPS(), "Created thread %d in pool \n", n);
        }
    }
    //Wait for threadPool to initialize
    while (newManager->num_threads_alive != N) {}
    return newManager;
}

//Initialize a thread in the thread pool
int threadPool_init(struct tp_manager_t* tpManager, struct thread_t** thread_p, int id)
{
    (*thread_p) = (struct thread_t*)malloc(sizeof(struct thread_t));
    if (thread_p == NULL) {
        perror("thread_init() ERROR: Could not allocate memory for thread\n");
        Log("thread_init() ERROR: Could not allocate memory for thread\n");
        return -1;
    }

    (*thread_p)->results = (struct taskRetData_t**)malloc(tpManager->F * sizeof (struct taskRetData_t*));
    if((*thread_p)->results == NULL) {
        perror("Could not allocate memory for results\n");
        Log("Could not allocate memory for results\n");
        return -1;
    }
    int i;
    for(i=0; i<tpManager->F; i++) {
        (*thread_p)->results[i] = (struct taskRetData_t*)malloc(1* sizeof(struct taskRetData_t));
        if((*thread_p)->results[i] == NULL) {
            perror("Could not allocate memory for buffer\n");
            Log("Could not allocate memory for buffer\n");
            return -1;
        }
        (*thread_p)->results[i]->size = 8;
    }
    //Initialize fiendly count & id.
    (*thread_p)->count = 0;
    (*thread_p)->id = id;
    (*thread_p)->myManager = tpManager;

    if (pthread_create(&(*thread_p)->threadID, NULL, (void *)doTask, *thread_p) != 0) {
        printf("pthread_create() ERROR: Could not create pthread_t %d\n", id);
        fprintf(getFPS(), "pthread_create() ERROR: Could not create pthread_t %d\n", id);
        return -1;
    }
    return 0;
}

//Add tast to the thread pool
int tpManager_addTask(struct tp_manager_t* tpManager, void* (*monteCarlo)(void*), void* arg_p)
{
    task_t* newtask;
    int i;
    newtask = (struct task_t*)malloc(sizeof (struct task_t));
    if (newtask == NULL) {
        perror("tpManager_addTask(): Could not allocate memory for new task\n");
        Log("tpManager_addTask(): Could not allocate memory for new task\n");
        return -1;
    }

    //add function and argument
    newtask->func = monteCarlo;
    newtask->arg = arg_p;

    long temp = tpManager->N;
    long temp2 = (long)newtask->arg;
    temp = temp2/temp;
    temp2 = temp2/temp;

    //add tasks to queue
    for(i=0; i<temp2; i++) {
        taskFeeder_push(&tpManager->taskFeeder, newtask, tpManager->F, tpManager->T);
        printf("Task Number %d Added to TaskFeeder.\n", i);
        fprintf(getFPS(), "Task Number %d Added to TaskFeeder.\n", i);
    }
    printf("Argumet %d was added to task.\n",(int)(tpManager->taskFeeder.front->arg));
    fprintf(getFPS(), "Argument %d was added to task.\n",(int)(tpManager->taskFeeder.front->arg));
    return 0;
}

//Taking the result from the doTask function that return from pthread_join and storing it to our dataPool.
void thread_join(struct tp_manager_t* tpManager)
{
    float temp;
    int i, j;
    void* res;
    tpManager->memOffset = 0;
    while(threads_keepalive) {
        printf("Entered to while function in thread_join function.\n");
        Log("Entered to while function in thread_join function.\n");

        for(j=0; j<tpManager->N; j++) {
            if (pthread_join(tpManager->threadPool[j]->threadID, &res) == 0 ) {
                printf("Join Matched with pthread: %d \n", pthread_self());
                fprintf(getFPS(), "Join Matched with pthread: %d \n", pthread_self());
            }
            if(res == NULL) {
                perror("ERROR: Return value from Join is NULL\n");
                Log("ERROR: Return value from Join is NULL\n");
            }
            struct thread_t* joinThread = (struct thread_t*)res;

            for (i = 0; i < tpManager->threadPool[j]->count; i++) {
                sprintf(&tpManager->dataPool[tpManager->memOffset], "%d", joinThread->results[i]->size);
                printf("Size %s Added to memOffset.\n", &tpManager->dataPool[tpManager->memOffset]);
                fprintf(getFPS(), "Size %s Added to memOffset.\n", &tpManager->dataPool[tpManager->memOffset]);

                tpManager->memOffset += sizeof(joinThread->results[i]->size);
                sprintf(&tpManager->dataPool[tpManager->memOffset], "%s", joinThread->results[i]->buffer);
                printf("Buffer %s Added to memOffset.\n", &tpManager->dataPool[tpManager->memOffset]);
                fprintf(getFPS(), "Buffer %s Added to memOffset.\n", &tpManager->dataPool[tpManager->memOffset]);

                tpManager->memOffset += joinThread->results[i]->size;
            }
        }
        break;
    }
    printf("EXIT FROM PTHREAD_JOIN.\n");
    Log("EXIT FROM PTHREAD_JOIN.\n");
    return NULL;
}

//Generating random number between 0-1.
float randNumGen()
{
    int random_value = rand();                          //Generate a random number
    float unit_random = random_value / (float)RAND_MAX; //make it between 0 and 1
    return unit_random;
}

//Monte-Carlo function to calculate PI
void* monteCarlo(long *I)
{
    printf("->IN MONTE CARLO<-\n");
    Log("->IN MONTE CARLO<-\n");

    //using malloc for the return variable in order make
    float *HITS = (float *)malloc(sizeof(float));
    *HITS = 0;

    //get the total number of iterations for a thread
    int counter = 0;

    //calculation
    for(counter = 0; counter< (*I); counter++) {
        float x = randNumGen();
        float y = randNumGen();
        float result = sqrt((x*x) + (y*y));
        if(result < 1)
            *HITS += 1;             //check if the generated value is inside a unit circle
    }
    printf("<-EXIT MONTE CARLO WITH HITS: %f -> \n", *HITS);
    fprintf(getFPS(), "<-EXIT MONTE CARLO WITH HITS: %f ->\n", *HITS);

    return HITS;     //return the HITS
}

//Destroy the threadpool
void destroy_manger(struct tp_manager_t* tpManager)
{
    printf("\nDestroying manager.\n");
    Log("\nDestroying manager.\n");
    //No need to destory if it's NULL
    if (tpManager == NULL) return;

    volatile int threads_total = tpManager->num_threads_alive;

    //End each thread 's infinite loop
    threads_keepalive = 0;

    //Job queue cleanup
    taskFeeder_destroy(&tpManager->taskFeeder);

    //Deallocs
    int n;
    for (n=0; n < threads_total; n++) {
        free(tpManager->threadPool[n]);
        printf("DELETING %d\n", n);
        fprintf(getFPS(), "DELETING %d\n", n);
    }
    free(tpManager->threadPool);
    free(tpManager);
}

//Main Application to run the threadpool.
int main(int argc, const char *argv[])
{
    float totHits, PI, HIT;
    char data[10], size[10];
    unsigned long i;
    tp_manager_t* tpManager;

    //Chceking if the arguments are currect
    if(atol(argv[1]) < 1 || atol(argv[2]) < 1 || atol(argv[3]) < 1 || atol(argv[4]) < 1) {
        perror("Invalid Input\n");
        Log("Invalid Input\n");
        return -1;
    }

    //Pointer to log file
    fp = fopen("./log.txt", "w");
    if (fp == NULL) {
        perror("Failed in FILE*. \n");
        return -1;
    }

    printf("** Roni Polisanov 307835884 **\n** POXIS Threads - Exercise 2 UNIX **\n** Lecturer: Mr. Roee Leon **\n** Shenkar College - High School of Engineering **\n \n");
    Log("** Roni Polisanov 307835884 **\n** POXIS Threads - Exercise 2 UNIX **\n** Lecturer: Mr. Roee Leon **\n** Shenkar College - High School of Engineering **\n \n");

    //Creating the ThreadPoolManager
    tpManager = create_manager(atol(argv[1]),atol(argv[2]),atol(argv[3]));

    //Initializing and adding tasks to taskFeeder
    tpManager_addTask(tpManager, monteCarlo, (void*)atol(argv[4]));

    //My first signal to INITIAL the "wait" Synchronization loop (broadcast alternative)
    pthread_cond_signal(&cond);

    //Joining to all threads
    pthread_join(pthread_self(), NULL);
    thread_join(tpManager);

    //Calculating the size and data
    for(i=4; i<(tpManager->memOffset); i+=12) {
        strncpy(size, &tpManager->dataPool[i-4], 4);
        printf("[Size=%c]", tpManager->dataPool[i-4]);
        fprintf(fp, "[Size=%c]", tpManager->dataPool[i-4]);

        strncpy(data, &tpManager->dataPool[i], 8);
        HIT = atof(data);
        printf("[Data=%f]\n", HIT);
        fprintf(fp, "[Data=%f]\n", HIT);
        totHits += HIT;
    }

    printf("Total hits: %f\n", totHits);
    fprintf(fp, "Total hits: %f\n", totHits);
    PI = (totHits/atol(argv[4]))*4;
    printf("\nThe Ratio of π is: %f\n", PI);
    fprintf(fp, "\nThe Ratio of π is: %f\n", PI);

    //Freeing all resources
    destroy_manger(tpManager);
    printf("Goodbye! =)\n");
    Log("Goodbye! =)\n");
    fclose(fp);
    return 0;
}