/*
 * chat_server.c — Chat Hub Server
 *
 * Architecture :
 *   - TCP  : connexions, messages privés, notifications, keepalive
 *   - UDP  : broadcast du salon général (envoi uniquement depuis le serveur)
 *   - select() : multiplexage sans blocage sur tous les descripteurs
 *   - Ping/Pong toutes les 15 s pour détecter les déconnexions brutales
 *
 * Compilation : gcc -Wall -pthread -o chat_serverd chat_server.c
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "../common/protocol.h"

/* ─── Constantes ─────────────────────────────────────── */
#define PING_INTERVAL   15   /* secondes entre deux pings               */
#define PING_TIMEOUT    30   /* secondes sans pong → déconnexion forcée */
#define BACKLOG         16

/* ─── État d'un client connecté ──────────────────────── */
typedef struct {
    int              fd;                  /* socket TCP                 */
    char             pseudo[MAX_PSEUDO];  /* pseudo validé              */
    struct sockaddr_in addr;              /* adresse IP:port UDP client */
    time_t           last_pong;          /* timestamp dernier pong     */
    int              active;             /* 1 = connecté               */
} Client;

/* ─── Globals ────────────────────────────────────────── */
static Client    clients[MAX_CLIENTS];
static int       nb_clients = 0;
static int       tcp_sock   = -1;
static int       udp_sock   = -1;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;

/* ─── Utilitaires ────────────────────────────────────── */

static void log_event(const char *fmt, ...) {
    va_list ap;
    time_t  t = time(NULL);
    struct  tm *tm = localtime(&t);
    char    ts[20];
    strftime(ts, sizeof(ts), "%H:%M:%S", tm);
    printf("[%s] ", ts);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
    fflush(stdout);
}

/* Envoie un Message TCP à un fd donné. Retourne -1 si erreur. */
static int send_tcp(int fd, const Message *m) {
    ssize_t n = send(fd, m, MSG_SIZE, MSG_NOSIGNAL);
    return (n == (ssize_t)MSG_SIZE) ? 0 : -1;
}

/* Construit et envoie un message système TCP à un fd. */
static void send_system(int fd, const char *text) {
    Message m;
    memset(&m, 0, sizeof(m));
    m.type = MSG_SYSTEM;
    strncpy(m.from, "SERVER", MAX_PSEUDO - 1);
    strncpy(m.body, text, MAX_MSG - 1);
    send_tcp(fd, &m);
}

/* Broadcast UDP vers tous les clients actifs sauf l'expéditeur. */
static void broadcast_udp(const Message *m, int sender_fd) {
    pthread_mutex_lock(&lock);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!clients[i].active) continue;
        if (clients[i].fd == sender_fd) continue;
        /* On envoie à l'adresse UDP enregistrée du client */
        sendto(udp_sock, m, MSG_SIZE, 0,
               (struct sockaddr *)&clients[i].addr,
               sizeof(clients[i].addr));
    }
    pthread_mutex_unlock(&lock);
}

/* Trouve un client par pseudo (NULL si absent). Appelé sous lock. */
static Client *find_client(const char *pseudo) {
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (clients[i].active && strcmp(clients[i].pseudo, pseudo) == 0)
            return &clients[i];
    return NULL;
}

/* Trouve un slot libre. Appelé sous lock. */
static int find_free_slot(void) {
    for (int i = 0; i < MAX_CLIENTS; i++)
        if (!clients[i].active) return i;
    return -1;
}

/* Construit la liste des pseudos connectés dans buf. */
static void build_users_list(char *buf, size_t size) {
    buf[0] = '\0';
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!clients[i].active) continue;
        strncat(buf, clients[i].pseudo, size - strlen(buf) - 2);
        strncat(buf, " ", size - strlen(buf) - 1);
    }
}

/* Notifie tous les clients TCP d'un événement système. */
static void notify_all(const char *text, int except_fd) {
    Message m;
    memset(&m, 0, sizeof(m));
    m.type = MSG_SYSTEM;
    strncpy(m.from, "SERVER", MAX_PSEUDO - 1);
    strncpy(m.body, text, MAX_MSG - 1);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!clients[i].active) continue;
        if (clients[i].fd == except_fd) continue;
        send_tcp(clients[i].fd, &m);
    }
}

/* Déconnecte proprement un client (appelé sous lock). */
static void disconnect_client(int idx) {
    if (!clients[idx].active) return;
    char notif[MAX_MSG];
    snprintf(notif, sizeof(notif), "%s a quitté le chat.", clients[idx].pseudo);
    log_event("%s", notif);
    clients[idx].active = 0;
    close(clients[idx].fd);
    clients[idx].fd = -1;
    nb_clients--;
    /* Notifier les autres (sans lock récursif) */
    Message m;
    memset(&m, 0, sizeof(m));
    m.type = MSG_SYSTEM;
    strncpy(m.from, "SERVER", MAX_PSEUDO - 1);
    strncpy(m.body, notif, MAX_MSG - 1);
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (!clients[i].active) continue;
        send_tcp(clients[i].fd, &m);
    }
}

/* ─── Thread keepalive ───────────────────────────────── */
static void *ping_thread(void *arg) {
    (void)arg;
    while (1) {
        sleep(PING_INTERVAL);
        time_t now = time(NULL);
        Message ping;
        memset(&ping, 0, sizeof(ping));
        ping.type = MSG_PING;

        pthread_mutex_lock(&lock);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (!clients[i].active) continue;
            /* Timeout dépassé ? */
            if (now - clients[i].last_pong > PING_TIMEOUT) {
                log_event("Timeout : %s déconnecté.", clients[i].pseudo);
                disconnect_client(i);
                continue;
            }
            send_tcp(clients[i].fd, &ping);
        }
        pthread_mutex_unlock(&lock);
    }
    return NULL;
}

/* ─── Traitement d'un message reçu d'un client TCP ──── */
static void handle_message(int idx, const Message *m) {
    Client *c = &clients[idx];

    switch (m->type) {

    case MSG_CONNECT: {
        /* Validation du pseudo */
        pthread_mutex_lock(&lock);
        int taken = (find_client(m->from) != NULL);
        if (!taken) {
            strncpy(c->pseudo, m->from, MAX_PSEUDO - 1);
            c->active     = 1;
            c->last_pong  = time(NULL);
            nb_clients++;
        }
        pthread_mutex_unlock(&lock);

        if (taken) {
            Message err;
            memset(&err, 0, sizeof(err));
            err.type = MSG_CONNECT_ERR;
            strncpy(err.body, "Pseudo déjà utilisé. Choisissez-en un autre.",
                    MAX_MSG - 1);
            send_tcp(c->fd, &err);
            return;
        }

        /* Stocker l'adresse UDP du client (port UDP = TCP port + 1 par convention) */
        /* Le client envoie son port UDP dans le champ body */
        int udp_port = atoi(m->body);
        if (udp_port > 0) {
            /* Récupérer l'IP depuis la socket TCP */
            struct sockaddr_in peer;
            socklen_t plen = sizeof(peer);
            getpeername(c->fd, (struct sockaddr *)&peer, &plen);
            c->addr = peer;
            c->addr.sin_port = htons((uint16_t)udp_port);
        }

        /* Confirmer */
        Message ok;
        memset(&ok, 0, sizeof(ok));
        ok.type = MSG_CONNECT_OK;
        strncpy(ok.from, "SERVER", MAX_PSEUDO - 1);
        snprintf(ok.body, MAX_MSG, "Bienvenue %s !", c->pseudo);
        send_tcp(c->fd, &ok);

        /* Notifier les autres */
        char notif[MAX_MSG];
        snprintf(notif, sizeof(notif), "%s a rejoint le chat.", c->pseudo);
        log_event("%s", notif);
        pthread_mutex_lock(&lock);
        notify_all(notif, c->fd);
        pthread_mutex_unlock(&lock);
        break;
    }

    case MSG_PUBLIC: {
        /* Relayer en UDP broadcast */
        log_event("[PUBLIC] %s: %s", c->pseudo, m->body);
        broadcast_udp(m, c->fd);
        break;
    }

    case MSG_PRIVATE: {
        pthread_mutex_lock(&lock);
        Client *dest = find_client(m->to);
        if (dest) {
            send_tcp(dest->fd, m);
            log_event("[PRIVÉ] %s → %s: %s", c->pseudo, m->to, m->body);
        } else {
            char err[MAX_MSG];
            snprintf(err, sizeof(err), "Utilisateur '%s' introuvable.", m->to);
            send_system(c->fd, err);
        }
        pthread_mutex_unlock(&lock);
        break;
    }

    case MSG_USERS: {
        char list[MAX_MSG] = "Connectés : ";
        pthread_mutex_lock(&lock);
        build_users_list(list + strlen(list),
                         sizeof(list) - strlen(list));
        pthread_mutex_unlock(&lock);
        send_system(c->fd, list);
        break;
    }

    case MSG_PONG: {
        pthread_mutex_lock(&lock);
        c->last_pong = time(NULL);
        pthread_mutex_unlock(&lock);
        break;
    }

    case MSG_DISCONNECT: {
        pthread_mutex_lock(&lock);
        disconnect_client(idx);
        pthread_mutex_unlock(&lock);
        break;
    }

    default:
        break;
    }
}

/* ─── Boucle principale select() ─────────────────────── */
static void server_loop(void) {
    fd_set read_fds;
    int    max_fd;

    while (1) {
        FD_ZERO(&read_fds);
        FD_SET(tcp_sock, &read_fds);
        max_fd = tcp_sock;

        pthread_mutex_lock(&lock);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].active && clients[i].fd > 0) {
                FD_SET(clients[i].fd, &read_fds);
                if (clients[i].fd > max_fd) max_fd = clients[i].fd;
            }
        }
        pthread_mutex_unlock(&lock);

        struct timeval tv = {1, 0};
        int ready = select(max_fd + 1, &read_fds, NULL, NULL, &tv);
        if (ready < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }

        /* Nouvelle connexion TCP */
        if (FD_ISSET(tcp_sock, &read_fds)) {
            struct sockaddr_in cli_addr;
            socklen_t cli_len = sizeof(cli_addr);
            int new_fd = accept(tcp_sock, (struct sockaddr *)&cli_addr, &cli_len);
            if (new_fd >= 0) {
                pthread_mutex_lock(&lock);
                int slot = find_free_slot();
                if (slot >= 0) {
                    memset(&clients[slot], 0, sizeof(Client));
                    clients[slot].fd        = new_fd;
                    clients[slot].active    = 0; /* pas encore authentifié */
                    clients[slot].last_pong = time(NULL);
                    log_event("Nouvelle connexion depuis %s:%d (slot %d)",
                              inet_ntoa(cli_addr.sin_addr),
                              ntohs(cli_addr.sin_port), slot);
                } else {
                    log_event("Serveur plein, connexion refusée.");
                    close(new_fd);
                }
                pthread_mutex_unlock(&lock);
            }
        }

        /* Données sur un client existant */
        pthread_mutex_lock(&lock);
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].fd <= 0) continue;
            if (!FD_ISSET(clients[i].fd, &read_fds)) continue;

            Message m;
            ssize_t n = recv(clients[i].fd, &m, MSG_SIZE, MSG_WAITALL);
            if (n <= 0) {
                /* Déconnexion brutale */
                if (clients[i].active)
                    log_event("Déconnexion brutale : %s", clients[i].pseudo);
                disconnect_client(i);
            } else {
                pthread_mutex_unlock(&lock);
                handle_message(i, &m);
                pthread_mutex_lock(&lock);
            }
        }
        pthread_mutex_unlock(&lock);
    }
}

/* ─── Main ───────────────────────────────────────────── */
int main(int argc, char *argv[]) {
    int tcp_port = DEFAULT_TCP_PORT;
    int udp_port = DEFAULT_UDP_PORT;

    for (int i = 1; i < argc - 1; i++) {
        if (strcmp(argv[i], "--port") == 0)
            tcp_port = atoi(argv[i + 1]);
    }
    udp_port = tcp_port + 1;

    /* ── Socket TCP ── */
    tcp_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (tcp_sock < 0) { perror("socket TCP"); exit(1); }
    int opt = 1;
    setsockopt(tcp_sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in srv = {0};
    srv.sin_family      = AF_INET;
    srv.sin_addr.s_addr = INADDR_ANY;
    srv.sin_port        = htons((uint16_t)tcp_port);

    if (bind(tcp_sock, (struct sockaddr *)&srv, sizeof(srv)) < 0) {
        perror("bind TCP"); exit(1);
    }
    if (listen(tcp_sock, BACKLOG) < 0) { perror("listen"); exit(1); }

    /* ── Socket UDP ── */
    udp_sock = socket(AF_INET, SOCK_DGRAM, 0);
    if (udp_sock < 0) { perror("socket UDP"); exit(1); }
    struct sockaddr_in usrv = {0};
    usrv.sin_family      = AF_INET;
    usrv.sin_addr.s_addr = INADDR_ANY;
    usrv.sin_port        = htons((uint16_t)udp_port);
    if (bind(udp_sock, (struct sockaddr *)&usrv, sizeof(usrv)) < 0) {
        perror("bind UDP"); exit(1);
    }

    memset(clients, 0, sizeof(clients));

    /* ── Thread keepalive ── */
    pthread_t pt;
    pthread_create(&pt, NULL, ping_thread, NULL);
    pthread_detach(pt);

    printf("╔══════════════════════════════════════╗\n");
    printf("║       Chat Hub Server démarré        ║\n");
    printf("╠══════════════════════════════════════╣\n");
    printf("║  TCP : %-5d   UDP : %-5d           ║\n", tcp_port, udp_port);
    printf("║  Clients max : %-3d                  ║\n", MAX_CLIENTS);
    printf("╚══════════════════════════════════════╝\n");
    fflush(stdout);

    server_loop();

    close(tcp_sock);
    close(udp_sock);
    return 0;
}
