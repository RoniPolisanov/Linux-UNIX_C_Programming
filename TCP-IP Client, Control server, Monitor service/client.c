/*
client socket for connection
*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <time.h>
#include <arpa/inet.h>

#define PORT "9034" // the port client will be connecting to
#define _OPEN_SYS_ITOA_EXT
#define MAXDATASIZE 4000 // max number of bytes we can get at once


// get sockaddr, IPv4 or IPv6:
void *get_in_addr(struct sockaddr *sa) {
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }
    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}


int main(int argc, char *argv[]) {
    struct timeval begin, end, total;
    struct addrinfo hints, *servinfo, *p;
    int sockfd, numbytes, temp, result, rv;
    char buf[MAXDATASIZE], s[INET6_ADDRSTRLEN], convert[15];

    if (argc != 2) {
        fprintf(stderr,"usage: client hostname\n");
        exit(1);
    }

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if ((rv = getaddrinfo(argv[1], PORT, &hints, &servinfo)) != 0) {
        fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
        return 1;
    }

    // loop through all the results and connect to the first we can
    //first step to establish the connection is to request by socket
    for(p = servinfo; p != NULL; p = p->ai_next) {
        if ((sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
            perror("client: socket");
            continue;
        }

        //after the connection has been established, we notify that the client has been connected
        if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
            perror("client: connect");
            close(sockfd);
            continue;
        }

        break;
    }

    if (p == NULL) {
        fprintf(stderr, "client: failed to connect\n");
        return 2;
    }

    inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr),s ,sizeof s);
    printf("client: connecting to %s\n", s);

    freeaddrinfo(servinfo); // all done with this structure
    //~~ HERE THE CLIENT HAS CONNECTED SUCCESFULLY! ~~//

    //the clients interface with the server in an endless loop
    while(1) {
        char buf[100];
        strcpy(buf, "START");
        // send the server time sample sequence egnite
        if (send(sockfd, buf, sizeof(buf), 0) == -1)
            perror("START");

        //starting time sampling
        gettimeofday(&begin, 0);

        //delaying the time sampling
        for(temp = 0; temp<100000000; temp++)
        {

        }
        //sample ending time
        gettimeofday(&end, 0);
        //calculate and convert the time sample

        printf("Total time in micro seconds = %ld\n", (1000000 * end.tv_sec + end.tv_usec) - (1000000 * begin.tv_sec + begin.tv_usec));

        /*if ((temp = recv(sockfd, buf, 5, 0)) == -1) {
        	perror("recv");
        	exit(1);
        }*/
        memset(convert, 0, 15);
        result = snprintf(buf,sizeof(buf),"%ld",(1000000 * end.tv_sec + end.tv_usec) - (1000000 * begin.tv_sec + begin.tv_usec));
        //`convert[strlen(convert)] = '\0';
        //send the server the time sample and end it
        if (send(sockfd, buf, sizeof(buf), 0) == -1)
            perror("totalTimeSend");

        sleep(3);
    }

    close(sockfd);

    return 0;
}
