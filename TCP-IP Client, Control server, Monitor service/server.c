/*
 ** selectserver.c -- a cheezy multiperson chat server
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/un.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <sys/time.h>

#define SOCK_PATH "echo_socket" //Admin Socket
#define PORT "9034" // port we're listening on
#define MAX_USER 99 // Max Users that can connect to the server
#define TRUE 1
#define FALSE 0

//client structure
typedef struct ourUSER {
    long int Samples[100];
    float averageSample;
    int numberOfSamples, insertIndex, connectionStatus, currFD;
    struct timeval begin;
} ourUSER;

//average structure
typedef struct avgArray {
    int index;
    float avg;
} avgArray;

// get sockaddr, IPv4 or IPv6:
void* get_in_addr(struct sockaddr * sa) {
    if (sa-> sa_family == AF_INET) {
        return &(((struct sockaddr_in * ) sa)-> sin_addr);
    }
    return &(((struct sockaddr_in6 * ) sa)-> sin6_addr);
}

//Clearing data of disconnected clients
void clearUser(ourUSER* USER,int j) {
    USER[j].connectionStatus = FALSE;
    USER[j].begin.tv_sec = USER[j].begin.tv_usec = 0;
    memset(USER[j].Samples, 0, sizeof(USER[j].Samples));
    USER[j].numberOfSamples = 0;
    USER[j].insertIndex = 0;
    USER[j].averageSample = 0.0;
}

//swap for Selection sort
void swap(avgArray* a, avgArray* b) {
    avgArray temp = *a;
    *a = *b;
    *b = temp;
}

//Selection sort
void averageSort(avgArray* avg, int size) {
    int i, j, maxIndex;
    for(i = 0; i < size; ++i) {
        maxIndex = i;
        for(j = i + 1; j < size; ++j) {
            if(avg[j].avg > avg[maxIndex].avg)
                maxIndex = j;
        }
        swap(&avg[i], &avg[maxIndex]);
    }
}


int main(void) {
    fd_set master; // master file descriptor list
    fd_set read_fds; // temp file descriptor list for select()
    struct sockaddr_storage remoteaddr; // client address
    struct sockaddr_un local, remote;
    struct addrinfo hints, * ai, * p;
    struct timeval end;
    socklen_t addrlen;
    char remoteIP[INET6_ADDRSTRLEN];
    char str[4000];
    char strtemp[100];
    char buf[100], token2[100]; // buffer for client data
    long int total;
    int fdmax; // maximum file descriptor number
    int s2; //admin fd
    int listener, UDSListener; // listening socket descriptor
    int newfd; // newly accept()ed socket descriptor
    int len, t, i, j, rv, k;
    int UDSnbytes, nbytes;
    int yes = 1; // for setsockopt() SO_REUSEADDR, below
    int CC4M, count = 0;    //counters for self checking connected clients
    char* token;

    ourUSER USER[MAX_USER] = {0};
    avgArray avgArr[MAX_USER];

    FD_ZERO(&master); // clear the master and temp sets
    FD_ZERO(&read_fds);

    // get us a socket and bind it
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    if ((rv = getaddrinfo(NULL, PORT, &hints, &ai)) != 0) {
        fprintf(stderr, "selectserver: %s\n", gai_strerror(rv));
        exit(1);
    }

    for (p = ai; p != NULL; p = p->ai_next) {
        listener = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (listener < 0)
            continue;

        // lose the pesky "address already in use" error message
        setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

        //binds the addrees to the listener
        if (bind(listener, p -> ai_addr, p -> ai_addrlen) < 0) {
            close(listener);
            continue;
        }
        break;
    }

    //create internal socket for the admin
    if ((UDSListener = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        perror("socket");
        exit(1);
    }

    local.sun_family = AF_UNIX;
    strcpy(local.sun_path, SOCK_PATH);
    unlink(local.sun_path);
    len = strlen(local.sun_path) + sizeof(local.sun_family);

    if (bind(UDSListener, (struct sockaddr*)&local, len) == -1) {
        perror("bind");
        exit(1);
    }

    if (listen(UDSListener, 1) == -1) {
        perror("listen");
        exit(1);
    }

    printf("Awaiting connections...\n");

    // if we got here, it means we didn't get bound
    if (p == NULL) {
        fprintf(stderr, "selectserver: failed to bind\n");
        exit(2);
    }

    freeaddrinfo(ai); // all done with this

    // this function allows you to connect maximum of 10 clients simultaneously
    if (listen(listener, 10) == -1) {
        perror("listen");
        exit(3);
    }

    // add the listener to the master set
    FD_SET(listener, &master);
    // add the UDSListener to the master set
    FD_SET(UDSListener, &master);

    // keep track of the latest(index) file descriptor
    fdmax = UDSListener; // so far, it's this one

    // main loop - awaiting users to connect
    for (;;) {
        read_fds = master; // copy it in order not to overwrite to the main FDList
        if (select(fdmax + 1, &read_fds, NULL, NULL, NULL) == -1) { //select choose the next available fd index
            perror("select");
            exit(4);
        }
        // run through the existing connections looking for data to read
        for (i = 0; i <= fdmax; i++) {
            if (FD_ISSET(i, & read_fds)) {  // we got one!!
                if (i == listener) {
                    // handle new connections
                    addrlen = sizeof remoteaddr;
                    for(j=0; j<MAX_USER; j++) {
                        if(USER[j].connectionStatus == FALSE) {
                            USER[j].currFD = newfd = accept(listener, (struct sockaddr*) &remoteaddr, &addrlen);
                            USER[j].connectionStatus = TRUE;
                            printf("USER[%d] Connected succefully\n", j);
                            break;
                        }
                        perror("Client list is full");
                    }

                    if (newfd == -1)
                        perror("accept");
                    else {
                        FD_SET(newfd, &master); // add to master set

                        if (newfd > fdmax) // keep track of the max
                            fdmax = newfd;

                        printf("selectserver: new connection from %s on "
                               "socket %d\n", inet_ntop(remoteaddr.ss_family, get_in_addr((struct sockaddr * ) & remoteaddr), remoteIP, INET6_ADDRSTRLEN), newfd);
                    }
                }     //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~END OF LISTENER CONNECTIONCHECK~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                else if (i == UDSListener) {  //admin wants to connect
                    t = sizeof(remote);
                    if ((s2 = accept(UDSListener, (struct sockaddr *)&remote, &t)) == -1) {
                        perror("accept");
                        exit(1);
                    }

                    FD_SET(s2, &master); // add to master set

                    if (s2 > fdmax) // keep track of the max
                        fdmax = s2;

                    printf("\n      ~~  <*((><  Admin Has Connected  ><((*>  ~~\n\n");
                } //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~END OF UDSLISTENER CONNECTION CHECK~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                else if ((i != s2) && ((nbytes = recv(i, buf, sizeof buf, 0)) <= 0)) {  //Client (not admin) want to disconnect from the server
                    if (nbytes == 0)  // connection closed
                        printf("selectserver: socket %d hung up\n", i);
                    else
                        perror("recv");

                    for (j = 0; j <= MAX_USER; ++j) {  //search for the user to diconnect
                        if (USER[j].currFD == i)
                            break;
                    }

                    clearUser(USER, j);     //Set user connection status to disconnected + clearing USer's data
                    printf("USER[%d] has disconnected.\n", j);

                    close(i); // bye!
                    FD_CLR(i, &master); // remove from master
                }  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~END OF DISCONNECT USER~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                else if (i != s2) { //Client Want to interact with the server (Time sample..)
                    if(nbytes > 0) {
                        for (j = 0; j <= MAX_USER; ++j) {  //search for the user trying to interact
                            if (USER[j].currFD == i)
                                break;
                        }

                        if (USER[j].begin.tv_sec == 0) {         //if user hasnt started a sample, start it
                            gettimeofday(&USER[j].begin, 0);
                            printf("~~~~~~~~~~~~~~~~Starting Time Sampling For Client %d~~~~~~~~~~~~~~~\n", USER[j].currFD);
                        }
                        else {
                            printf("The time sample recieved from the client #%d: %s\n", USER[j].currFD, buf);

                            gettimeofday(&end, 0);
                            total = (1000000 * end.tv_sec + end.tv_usec) - (1000000 * USER[j].begin.tv_sec + USER[j].begin.tv_usec);

                            printf("The time sample of the local Server for client #%d: %ld\n", USER[j].currFD, total);

                            if(USER[j].insertIndex == 99)        //if insert index has reached max
                                USER[j].insertIndex = 0;             //reset the insertIndex

                            USER[j].Samples[USER[j].insertIndex] = abs(total - atof(buf));  //insert the new sample into the sample array
                            ++(USER[j].insertIndex);                        //promote insertIndex

                            if (USER[j].numberOfSamples < 100)            //check if user has reached 100 samples
                                ++(USER[j].numberOfSamples);                // if not, promote sample count

                            //Calculating the overhead average of each Client
                            for(k=0; k<USER[j].numberOfSamples; ++k)
                                USER[j].averageSample += USER[j].Samples[k];

                            USER[j].averageSample = ((USER[j].averageSample) / (USER[j].numberOfSamples));  //Average calculate
                            USER[j].begin.tv_sec = 0;                            //reset begin time
                            printf("The Overhead time between the Server and client #%d: %ld\n", USER[j].currFD, (USER[j].Samples[(USER[j].insertIndex) - 1]));
                        }
                    }
                } // END handle data from client
                else if(((UDSnbytes = recv(s2, str, 100, 0) > 0) && !strcmp(str, "disconnect\n"))) {  //here we know that the ClientAdmin want to disconnect
                    printf("\n      ~~  <*((><  Admin Has Disconnected  ><((*>  ~~\n\n");
                    close(s2);
                    FD_CLR(s2, &master);
                }  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`ADMIN WANTS TO DIScONNECT~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
                else if ((UDSnbytes > 0) && (i == s2)) { //here we know that the Admin wants to make a query
                  //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~whois command~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    if(strncmp(str,"whois", 5) == 0) {
                        memset(str, 0, 4000);
                        memset(strtemp, 0, 100);
                        strcat(str, "Clients and last recorded sample \n");
                        for (j = 0; j <= MAX_USER; ++j) {
                            if (USER[j].connectionStatus == TRUE) {
                                snprintf(strtemp, 10, "%d", USER[j].currFD);
                                strcat(str, "Client Socket: ");
                                strcat(str, strtemp);
                                strcat(str, ", Last Sample: ");
                                snprintf(strtemp, 10, "%ld", USER[j].Samples[USER[j].insertIndex - 1]);
                                strcat(str, strtemp);
                                strcat(str, ".\n");
                                CC4M = TRUE;
                            }
                        }
                        if(!CC4M) {
                            memset(str, 0, 4000);
                            strcat(str, "No client sample available yet.\n");
                            if (send(s2, str, strlen(str), 0) < 0)
                                perror("whois - send");
                        }
                        else {
                            if (send(s2, str, strlen(str), 0) < 0)
                                perror("whois - send");
                        }
                    }//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~getworst command~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    else if(strncmp(str, "getworst", 8) == 0) {
                        memset(str, 0, 4000);
                        memset(strtemp, 0, 100);

                        for(k = 0; k <= MAX_USER; ++k) {
                            if(USER[k].connectionStatus == TRUE) {
                                avgArr[count].avg = USER[k].averageSample;
                                avgArr[count].index = USER[k].currFD;
                                ++count;
                            }
                        }
                        if(count > 1)
                            averageSort(avgArr, count);
                        //count *= 0.05;            //Five precent from the clients under 20 is less than one - SO we decided to send you the worst 5 users (100*0.05 = 5)
                        strcat(str, "Clients with the worst sample average \n");
                        for(k = 1; (k <= count); ++k) {
                          if (avgArr[k-1].index != NULL){
                            snprintf(strtemp, 10, "%d", k);
                            strcat(str, strtemp);
                            strcat(str, ". Client Socket: ");
                            snprintf(strtemp, 10, "%d", avgArr[k-1].index);
                            strcat(str, strtemp);
                            strcat(str, ", Average Sample: ");
                            snprintf(strtemp, 10, "%f", avgArr[k-1].avg);
                            strcat(str, strtemp);
                            strcat(str, ".\n");
                          }
                        }
                        if (count > 0) {
                            if (send(s2, str, strlen(str), 0) < 0)
                                perror("getworst - send");
                        }
                        else {
                            memset(str, 0, 4000);
                            strcat(str, "No client sample available yet.\n");
                            if (send(s2, str, strlen(str), 0) < 0)
                                perror("whois - send");
                        }
                    }//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~grep <client socket> command~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    else if(strncmp(str, "grep ", 5) == 0) {
                          token = strtok(str, " ");
                          token = strtok(NULL, "\n");
                          memcpy(token2, token, strlen(token) + 1);
                          memset(str, 0, 4000);
                          memset(strtemp, 0, 100);
                          for(k = 0; k <= MAX_USER; ++k) {
                              if(USER[k].connectionStatus == TRUE) {
                                  for(j = 0; j < USER[k].numberOfSamples; ++j) {
                                      snprintf(strtemp, 10, "%ld", USER[k].Samples[j]);
                                      if(strstr(strtemp, token2) != NULL) {
                                          snprintf(strtemp, 10, "%d", USER[k].currFD);
                                          strcat(str, "Client Socket: ");
                                          strcat(str, strtemp);
                                          strcat(str, ", Sample: ");
                                          snprintf(strtemp, 10, "%ld", USER[k].Samples[j]);
                                          strcat(str, strtemp);
                                          strcat(str, ".\n");
                                      }
                                  }
                                  snprintf(strtemp, 10, "%f", USER[k].averageSample);
                                  if(strstr(strtemp, token2) != NULL) {
                                      snprintf(strtemp, 10, "%d", USER[k].currFD);
                                      strcat(str, "Client Socket: ");
                                      strcat(str, strtemp);
                                      strcat(str, ", Average Sample: ");
                                      snprintf(strtemp, 10, "%f", USER[k].averageSample);
                                      strcat(str, strtemp);
                                      strcat(str, ".\n");
                                  }
                              }
                          }
                          if (strlen(str) != 0){
                              if (send(s2, str, strlen(str), 0) < 0)
                                  perror("grep - send");
                          }
                          memset(str, 0, 4000);
                          strcat(str, "grep for ARGS: ");
                          strcat(str, token2);
                          strcat(str, " DONE.\n");
                          if (send(s2, str, strlen(str), 0) < 0)
                              perror("grep - send");
                    }//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Unknown command~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    else {
                        memset(strtemp, 0, 100);
                        strcat(strtemp, "Unknown command: ");
                        strcat(strtemp, str);
                        if (send(s2, strtemp, strlen(strtemp), 0) < 0)
                            perror("grep - send");
                    }//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~clearing temporary variables and counters~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
                    memset(str, 0, 4000);
                    memset(strtemp, 0, 100);
                    CC4M = 0;
                    count = 0;
                }
            } // END got new incoming connection
        } // END looping through file descriptors
    } // END for(;;)--and you thought it would never end!
    return 0;
}
