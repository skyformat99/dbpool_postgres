#ifndef ZPGSQL_H
#define ZPGSQL_H

#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 700
#endif

#include <semaphore.h>
#include "libpq-fe.h"

typedef enum {
    zFalse = 0,
    zTrue = 1
} zbool_t;

#define _s signed short int
#define _us unsigned short int
#define _i signed int
#define _ui unsigned int
#define _l signed long int
#define _ul unsigned long int
#define _ll signed long long int
#define _ull unsigned long long int

#define _f float
#define _d double

#define _c signed char
#define _uc unsigned char

#define /*_i*/ zPRINT_TIME(/*void*/) {\
    time_t MarkNow = time(NULL);  /* Mark the time when this process start */\
    struct tm *pCurrentTime_ = localtime(&MarkNow);  /* Current time(total secends from 1900-01-01 00:00:00) */\
    fprintf(stderr, "\033[31m[ %d-%d-%d %d:%d:%d ]\033[00m",\
            pCurrentTime_->tm_year + 1900,\
            pCurrentTime_->tm_mon + 1,  /* Month (0-11) */\
            pCurrentTime_->tm_mday,\
            pCurrentTime_->tm_hour,\
            pCurrentTime_->tm_min,\
            pCurrentTime_->tm_sec);\
}

#define zPRINT_ERR(zErrNo, zCause, zMsg) do {\
    zPRINT_TIME();\
    fprintf(stderr,\
    "\033[31;01m[ ERROR ] \033[00m"\
    "\033[31;01m__FIlE__:\033[00m %s; "\
    "\033[31;01m__LINE__:\033[00m %d; "\
    "\033[31;01m__FUNC__:\033[00m %s; "\
    "\033[31;01m__CAUSE__:\033[00m %s; "\
    "\033[31;01m__DETAIL__:\033[00m %s\n",\
    __FILE__,\
    __LINE__,\
    __func__,\
    NULL == (zCause) ? "" : (zCause),\
    NULL == (zCause) ? (NULL == (zMsg) ? "" : (zMsg)) : strerror(zErrNo));\
} while(0)

#define zCHECK_NULL_EXIT(zRes) do{\
    if (NULL == (zRes)) {\
        zPRINT_ERR(errno, #zRes " == NULL", "");\
        exit(1);\
    }\
} while(0)

#define zMEM_ALLOC(zpRet, zType, zCnt) do {\
    zCHECK_NULL_EXIT( zpRet = malloc((zCnt) * sizeof(zType)) );\
} while(0)

typedef PGconn zPgConnHd__;
typedef PGresult zPgResHd__;

typedef struct __zPgResTuple__ {
    //_i *p_taskCnt;

    char **pp_fields;
} zPgResTuple__;

typedef struct __zPgRes__ {
    zPgResHd__ *p_pgResHd_;

    _i tupleCnt;
    _i fieldCnt;

    //_i taskCnt;

    zPgResTuple__ fieldNames_;
    zPgResTuple__ tupleRes_[];
} zPgRes__;

struct zPgSQL__ {
    zPgConnHd__ * (* conn) (const char *);
    void (* conn_reset) (zPgConnHd__ *);

    zPgResHd__ * (* exec) (zPgConnHd__ *, const char *, zbool_t);
    zPgResHd__ * (* exec_with_param) (zPgConnHd__ *, const char *, _i, const char * const *, zbool_t);
    zPgResHd__ * (* prepare) (zPgConnHd__ *, const char *, const char *, _i);
    zPgResHd__ * (* prepare_exec) (zPgConnHd__ *, const char *, _i, const char * const *, zbool_t);

    zPgRes__ * (* parse_res) (zPgResHd__ *);

    void (* res_clear) (zPgResHd__ *, zPgRes__ *);
    void (* conn_clear) (zPgConnHd__ *);

    zbool_t (* thread_safe_check) ();
    zbool_t (* conn_check) (const char *);

    _i (* exec_once) (char *, char *, zPgRes__ **);
    _i (* exec_with_param_once) (char *, char *, _i, const char **, zPgRes__ **);

    void (* conn_pool_init) (char *);
    _i (* write_db) (void *);
};

#endif  // #ifndef ZPGSQL_H
