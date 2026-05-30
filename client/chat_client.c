/*
 * chat_client.c — Chat Hub Client
 *
 * Architecture :
 *   - Thread principal  : lecture stdin → envoi TCP (messages privés,
 *                         commandes) ou UDP (salon général)
 *   - Thread réception  : recv TCP (privés, notifs, ping) + recv UDP (public)
 *
 * Commandes disponibles :
 *   /msg <pseudo> <texte>  — message privé
 *   /users                 — liste des connectés
 *   /quit                  — déconnexion propre
 *   <texte>                — message public (salon général)
 *
 * Compilation : gcc -Wall -pthread -o chat_client chat_client.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "../common/protocol.h"

/* ─── Globals ────────────────────────────────────────── */
static int   tcp_fd   = -1;
static int   udp_fd   = -1;
static char  my_pseudo[MAX_PSEUDO];
static int   running  = 1;

static struct sockaddr_in srv_tcp_addr;
static struct sockaddr_in srv_udp_addr;  /* pour sendto UDP */
static int   udp_local_port;

/* ─── Affichage ──────────────────────────────────────── */
static void print_msg(const char *prefix, const char *from,
                      const char *body, int is_private) {
    if (is_private)
        printf("\033[1;35m[PRIVÉ de %s]\033[0m %s\n", from, body);
    else if (prefix)
        printf("\033[1;33m%s\033[0m %s\n", prefix, body);
    else
        printf("\033[1;36m%s\033[0m: %s\n", from, body);
    fflush(stdout);
}

static void print_system(const char *body) {
    printf("\033[1;32m[INFO]\033[0m %s\n", body);
    fflush(stdout);
}

/* ─── Envoi TCP ──────────────────────────────────────── */
static int send_tcp(const Message *m) {
    ssize_t n = send(tcp_fd, m, MSG_SIZE, 0);
    return (n == (ssize_t)MSG_SIZE) ? 0 : -1;
}

/* ─── Thread réception (TCP + UDP via select) ─────────── */
static void *recv_thread(void *arg) {
    (void)arg;
    fd_set fds;
    int maxfd = (tcp_fd > udp_fd) ? tcp_fd : udp_fd;

    while (running) {
        FD_ZERO(&fds);
        FD_SET(tcp_fd, &fds);
        FD_SET(udp_fd, &fds);

        struct timeval tv = {1, 0};
        int r = select(maxfd + 1, &fds, NULL, NULL, &tv);
        if (r < 0) { if (errno == EINTR) continue; break; }
        if (r == 0) continue;

        /* ── TCP ── */
        if (FD_ISSET(tcp_fd, &fds)) {
            Message m;
            ssize_t n = recv(tcp_fd, &m, MSG_SIZE, MSG_WAITALL);
            if (n <= 0) {
                print_system("Connexion perdue avec le serveur.");
                running = 0;
                break;
            }
            switch (m.type) {
            case MSG_CONNECT_OK:
                print_system(m.body);
                break;
            case MSG_CONNECT_ERR:
                print_system(m.body);
                running = 0;
                break;
            case MSG_SYSTEM:
                print_system(m.body);
                break;
            case MSG_PRIVATE:
                print_msg(NULL, m.from, m.body, 1);
                break;
            case MSG_PING: {
                Message pong;
                memset(&pong, 0, sizeof(pong));
                pong.type = MSG_PONG;
                strncpy(pong.from, my_pseudo, MAX_PSEUDO - 1);
                send_tcp(&pong);
                break;
            }
            default:
                break;
            }
        }

        /* ── UDP (messages publics) ── */
        if (FD_ISSET(udp_fd, &fds)) {
            Message m;
            struct sockaddr_in src;
            socklen_t slen = sizeof(src);
            ssize_t n = recvfrom(udp_fd, &m, MSG_SIZE, 0,
                                 (struct sockaddr *)&src, &slen);
            if (n > 0 && m.type == MSG_PUBLIC) {
                print_msg(NULL, m.from, m.body, 0);
            }
        }
    }
    return NULL;
}

/* ─── Connexion initiale au serveur ──────────────────── */
static int connect_to_server(const char *host, int tcp_port) {
    /* TCP */
    tcp_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_fd < 0) { perror("socket TCP"); return -1; }

    memset(&srv_tcp_addr, 0, sizeof(srv_tcp_addr));
    srv_tcp_addr.sin_family = AF_INET;
    srv_tcp_addr.sin_port   = htons((uint16_t)tcp_port);
    if (inet_pton(AF_INET, host, &srv_tcp_addr.sin_addr) <= 0) {
        fprintf(stderr, "Adresse IP invalide : %s\n", host);
        return -1;
    }
    if (connect(tcp_fd, (struct sockaddr *)&srv_tcp_addr,
                sizeof(srv_tcp_addr)) < 0) {
        perror("connect TCP");
        return -1;
    }

    /* UDP local (réception broadcasts) */
    udp_fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_fd < 0) { perror("socket UDP"); return -1; }

    struct sockaddr_in local_udp = {0};
    local_udp.sin_family      = AF_INET;
    local_udp.sin_addr.s_addr = INADDR_ANY;
    local_udp.sin_port        = 0; /* port attribué automatiquement */
    if (bind(udp_fd, (struct sockaddr *)&local_udp, sizeof(local_udp)) < 0) {
        perror("bind UDP"); return -1;
    }
    /* Récupérer le port attribué */
    socklen_t slen = sizeof(local_udp);
    getsockname(udp_fd, (struct sockaddr *)&local_udp, &slen);
    udp_local_port = ntohs(local_udp.sin_port);

    /* Adresse UDP du serveur (port TCP + 1) */
    memset(&srv_udp_addr, 0, sizeof(srv_udp_addr));
    srv_udp_addr.sin_family = AF_INET;
    srv_udp_addr.sin_port   = htons((uint16_t)(tcp_port + 1));
    inet_pton(AF_INET, host, &srv_udp_addr.sin_addr);

    return 0;
}

/* ─── Authentification ───────────────────────────────── */
static int authenticate(void) {
    printf("Pseudo : ");
    fflush(stdout);
    if (!fgets(my_pseudo, sizeof(my_pseudo), stdin)) return -1;
    my_pseudo[strcspn(my_pseudo, "\n")] = '\0';
    if (strlen(my_pseudo) == 0) { fprintf(stderr, "Pseudo vide.\n"); return -1; }

    Message m;
    memset(&m, 0, sizeof(m));
    m.type = MSG_CONNECT;
    strncpy(m.from, my_pseudo, MAX_PSEUDO - 1);
    /* Envoyer le port UDP local dans body */
    snprintf(m.body, MAX_MSG, "%d", udp_local_port);
    return send_tcp(&m);
}

/* ─── Boucle stdin ───────────────────────────────────── */
static void input_loop(void) {
    char line[MAX_MSG + MAX_PSEUDO + 10];

    printf("\nCommandes : /msg <pseudo> <texte>  |  /users  |  /quit\n");
    printf("────────────────────────────────────────────────────────\n");

    while (running) {
        printf("%s> ", my_pseudo);
        fflush(stdout);

        if (!fgets(line, sizeof(line), stdin)) break;
        line[strcspn(line, "\n")] = '\0';
        if (strlen(line) == 0) continue;

        Message m;
        memset(&m, 0, sizeof(m));
        strncpy(m.from, my_pseudo, MAX_PSEUDO - 1);

        if (strcmp(line, "/quit") == 0) {
            m.type = MSG_DISCONNECT;
            send_tcp(&m);
            running = 0;
            break;

        } else if (strcmp(line, "/users") == 0) {
            m.type = MSG_USERS;
            send_tcp(&m);

        } else if (strncmp(line, "/msg ", 5) == 0) {
            /* /msg <pseudo> <texte> */
            char *rest = line + 5;
            char *sp   = strchr(rest, ' ');
            if (!sp) {
                printf("Usage : /msg <pseudo> <texte>\n");
                continue;
            }
            *sp = '\0';
            strncpy(m.to,   rest, MAX_PSEUDO - 1);
            strncpy(m.body, sp + 1, MAX_MSG - 1);
            m.type = MSG_PRIVATE;
            send_tcp(&m);

        } else {
            /* Message public → UDP */
            m.type = MSG_PUBLIC;
            strncpy(m.body, line, MAX_MSG - 1);
            sendto(udp_fd, &m, MSG_SIZE, 0,
                   (struct sockaddr *)&srv_udp_addr, sizeof(srv_udp_addr));
        }
    }
}

/* ─── Main ───────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    const char *host     = "127.0.0.1";
    int         tcp_port = DEFAULT_TCP_PORT;

    for (int i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], "--server") == 0) host     = argv[i + 1];
        if (strcmp(argv[i], "--port")   == 0) tcp_port = atoi(argv[i + 1]);
    }

    printf("Connexion à %s:%d ...\n", host, tcp_port);
    if (connect_to_server(host, tcp_port) < 0) {
        fprintf(stderr, "Impossible de se connecter.\n");
        exit(1);
    }
    printf("Connecté.\n");

    if (authenticate() < 0) {
        fprintf(stderr, "Erreur d'authentification.\n");
        exit(1);
    }

    /* Attendre MSG_CONNECT_OK ou MSG_CONNECT_ERR dans le thread recv */
    pthread_t rt;
    pthread_create(&rt, NULL, recv_thread, NULL);

    /* Petite pause pour recevoir la réponse du serveur */
    usleep(200000);
    if (!running) {
        pthread_join(rt, NULL);
        close(tcp_fd);
        close(udp_fd);
        exit(1);
    }

    input_loop();

    running = 0;
    pthread_join(rt, NULL);
    close(tcp_fd);
    close(udp_fd);

    printf("Déconnecté. À bientôt !\n");
    return 0;
}
