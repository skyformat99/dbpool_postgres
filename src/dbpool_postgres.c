#include "dbpool_postgres.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>
#include <errno.h>

static zPgConnHd__ * zpg_conn(const char *zpConnInfo);
static void zpg_conn_reset(zPgConnHd__ *zpPgConnHd_);
static zPgResHd__ * zpg_exec(zPgConnHd__ *zpPgConnHd_, const char *zpSQL, zbool_t zNeedRet);
static zPgResHd__ * zpg_exec_with_param(zPgConnHd__ *zpPgConnHd_, const char *zpCmd, _i zParamCnt, const char * const *zppParamValues, zbool_t zNeedRet);
static zPgResHd__ * zpg_prepare(zPgConnHd__ *zpPgConnHd_, const char *zpSQL, const char *zpPreObjName, _i zParamCnt);
static zPgResHd__ * zpg_prepare_exec(zPgConnHd__ *zpPgConnHd_, const char *zpPreObjName, _i zParamCnt, const char * const *zppParamValues, zbool_t zNeedRet);
static zPgRes__ * zpg_parse_res(zPgResHd__ *zpPgResHd_);

static void zpg_res_clear(zPgResHd__ *zpPgResHd_, zPgRes__ *zpPgRes_);
static void zpg_conn_clear(zPgConnHd__ *zpPgConnHd_);
static zbool_t zpg_conn_check(const char *zpConnInfo);
static zbool_t zpg_thread_safe_check();

static _i zpg_exec_once(char *zpConnInfo, char *zpCmd, zPgRes__ **zppPgRes_);
static _i zpg_exec_with_param_once(char *zpConnInfo, char *zpCmd, _i zParamCnt, const char **zppParam, zPgRes__ **zppPgRes_);

static void zpg_conn_pool_init(char *zpConnInfo);

static _i zwrite_db(void *zp);

/* postgreSQL 连接池 */
#define zDB_POOL_SIZ 16
static pthread_mutex_t zDBPoolLock = PTHREAD_MUTEX_INITIALIZER;
static zPgConnHd__ *zpDBPool_[zDB_POOL_SIZ] = { NULL };
static _s zDBPoolStack[zDB_POOL_SIZ];
static _s zDBPoolStackHeader;


/* Public 接口 */
struct zPgSQL__ zPgSQL_ = {
    .conn = zpg_conn,
    .conn_reset = zpg_conn_reset,

    .exec = zpg_exec,
    .exec_with_param = zpg_exec_with_param,
    .prepare = zpg_prepare,
    .prepare_exec = zpg_prepare_exec,

    .parse_res = zpg_parse_res,

    .res_clear = zpg_res_clear,
    .conn_clear = zpg_conn_clear,

    .thread_safe_check = zpg_thread_safe_check,
    .conn_check = zpg_conn_check,

    .exec_once = zpg_exec_once,
    .exec_with_param_once = zpg_exec_with_param_once,

    .conn_pool_init = zpg_conn_pool_init,
    .write_db = zwrite_db,
};

/* DB 连接池初始化 */
static void
zpg_conn_pool_init(char *zpConnInfo) {
    zDBPoolStackHeader = zDB_POOL_SIZ - 1;

    for (_i i = 0; i < zDB_POOL_SIZ; i++) {
        zDBPoolStack[i] = i;

        if (NULL == (zpDBPool_[i] = zPgSQL_.conn(zpConnInfo))) {
            fprintf(stderr, "%s, %d: pg_conn err", __FILE__, __LINE__);
            exit(1);
        }
    }
}

/* 连接 pgSQL server */
static zPgConnHd__ *
zpg_conn(const char *zpConnInfo) {
    zPgConnHd__ *zpPgConnHd_ = PQconnectdb(zpConnInfo);
    if (CONNECTION_OK == PQstatus(zpPgConnHd_)) {
        return zpPgConnHd_;
    } else {
        fprintf(stderr, "%s, %d: %s", __FILE__, __LINE__, PQerrorMessage(zpPgConnHd_));
        PQfinish(zpPgConnHd_);
        return NULL;
    }
}


/* 断线重连 */
static void
zpg_conn_reset(zPgConnHd__ *zpPgConnHd_) {
    PQreset(zpPgConnHd_);
}


/*
 * 执行 SQL cmd
 * zNeedRet 为 zTrue 时，表时此 SQL 属于查询类，有结果需要返回
 * */
static zPgResHd__ *
zpg_exec(zPgConnHd__ *zpPgConnHd_, const char *zpSQL, zbool_t zNeedRet) {
    zPgResHd__ *zpPgResHd_ = PQexec(zpPgConnHd_, zpSQL);
    if ((zTrue == zNeedRet ? PGRES_TUPLES_OK : PGRES_COMMAND_OK) == PQresultStatus(zpPgResHd_)) {
        return zpPgResHd_;
    } else {
        fprintf(stderr, "%s, %d: %s", __FILE__, __LINE__, PQresultErrorMessage(zpPgResHd_));
        PQclear(zpPgResHd_);
        return NULL;
    }
}


/* 使用带外部参数的方式执行 SQL cmd */
static zPgResHd__ *
zpg_exec_with_param(zPgConnHd__ *zpPgConnHd_, const char *zpCmd, _i zParamCnt, const char * const *zppParamValues, zbool_t zNeedRet) {
    zPgResHd__ *zpPgResHd_ = PQexecParams(zpPgConnHd_, zpCmd, zParamCnt, NULL, zppParamValues, NULL, NULL, 0);
    if ((zTrue == zNeedRet ? PGRES_TUPLES_OK : PGRES_COMMAND_OK) == PQresultStatus(zpPgResHd_)) {
        return zpPgResHd_;
    } else {
        fprintf(stderr, "%s, %d: %s", __FILE__, __LINE__, PQresultErrorMessage(zpPgResHd_));
        PQclear(zpPgResHd_);
        return NULL;
    }
}


/* 预编译重复执行的 SQL cmd，加快执行速度 */
static zPgResHd__ *
zpg_prepare(zPgConnHd__ *zpPgConnHd_, const char *zpSQL, const char *zpPreObjName, _i zParamCnt) {
    zPgResHd__ *zpPgResHd_ = PQprepare(zpPgConnHd_, zpPreObjName, zpSQL, zParamCnt, NULL);
    if (PGRES_COMMAND_OK == PQresultStatus(zpPgResHd_)) {
        return zpPgResHd_;
    } else {
        fprintf(stderr, "%s, %d: %s", __FILE__, __LINE__, PQresultErrorMessage(zpPgResHd_));
        PQclear(zpPgResHd_);
        return NULL;
    }
}


/* 使用预编译的 SQL 对象快速执行 SQL cmd */
static zPgResHd__ *
zpg_prepare_exec(zPgConnHd__ *zpPgConnHd_, const char *zpPreObjName, _i zParamCnt, const char * const *zppParamValues, zbool_t zNeedRet) {
    zPgResHd__ *zpPgResHd_ = PQexecPrepared(zpPgConnHd_, zpPreObjName, zParamCnt, zppParamValues, NULL, NULL, 0);
    if ((zTrue == zNeedRet ? PGRES_TUPLES_OK : PGRES_COMMAND_OK) == PQresultStatus(zpPgResHd_)) {
        return zpPgResHd_;
    } else {
        fprintf(stderr, "%s, %d: %s", __FILE__, __LINE__, PQresultErrorMessage(zpPgResHd_));
        PQclear(zpPgResHd_);
        return NULL;
    }
}


/*
 * 解析并输出 SQL 查询类命令返回的结果
 * 返回的数据中，第一组是字段名称
 * 返回 NULL 表示查询结果为空
 */
static zPgRes__ *
zpg_parse_res(zPgResHd__ *zpPgResHd_) {
    _i zTupleCnt = 0,
       zFieldCnt = 0,
       i = 0,
       j = 0;
    zPgRes__ *zpPgRes_ = NULL;

    if (0 == (zTupleCnt = PQntuples(zpPgResHd_))) {
        return NULL;
    }
    zFieldCnt = PQnfields(zpPgResHd_);

    zMEM_ALLOC(zpPgRes_, char, sizeof(zPgRes__) + zTupleCnt * sizeof(zPgResTuple__)
            + zFieldCnt * sizeof(void *)  /* for zpPgRes_->fieldNames_.pp_fields */
            + zTupleCnt * (zFieldCnt * sizeof(void *)));  /* for zpPgRes_->tupleRes_[0].pp_fields */

    zpPgRes_->fieldNames_.pp_fields = (char **) (
            (char *) (zpPgRes_ + 1) + zTupleCnt * sizeof(zPgResTuple__)
            );

    zpPgRes_->tupleRes_[0].pp_fields = zpPgRes_->fieldNames_.pp_fields + zFieldCnt;

    zpPgRes_->tupleCnt = zTupleCnt;
    zpPgRes_->fieldCnt = zFieldCnt;

    /* 提取字段名称 */
    for (i = 0; i < zFieldCnt; i++) {
        zpPgRes_->fieldNames_.pp_fields[i] = PQfname(zpPgResHd_, i);
    }

    /* 提取每一行各字段的值 */
    for (j = 0; j < zTupleCnt; j++) {
        zpPgRes_->tupleRes_[j].pp_fields = zpPgRes_->tupleRes_[0].pp_fields + j * zFieldCnt;
        for (i = 0; i < zFieldCnt; i++) {
            zpPgRes_->tupleRes_[j].pp_fields[i] = PQgetvalue(zpPgResHd_, j, i);
        }
    }

    return zpPgRes_;
}


/*
 * 清理 SQL 查询结果相关资源
 * !!! zpPgRes_ 中引用了 zpPgResHd_ 中的数据，
 * 故两者必须同时清理，不能在清理前者后，继续使用后者 !!!
 */
static void
zpg_res_clear(zPgResHd__ *zpPgResHd_, zPgRes__ *zpPgRes_) {
    /* 顺序不可变 */
    if (NULL != zpPgResHd_) {
        PQclear(zpPgResHd_);
    };

    if (NULL != zpPgRes_) {
        free(zpPgRes_);
    }
}


/* 清理 pgSQL 连接句柄 */
static void
zpg_conn_clear(zPgConnHd__ *zpPgConnHd_) {
    if (NULL != zpPgConnHd_) {
        PQfinish(zpPgConnHd_);
    }
}


/* 检查所在环境是否是线程安全的 */
static zbool_t
zpg_thread_safe_check() {
    return 1 == PQisthreadsafe() ? zTrue : zFalse;
}


/* 测试 pgSQL 服务器是否正常连接 */
static zbool_t
zpg_conn_check(const char *zpConnInfo) {
    return PQPING_OK == PQping(zpConnInfo) ? zTrue : zFalse;
}


/* 执行一次性 SQL cmd 的封装 */
static _i
zpg_exec_once(char *zpConnInfo, char *zpCmd, zPgRes__ **zppPgRes_) {
    zPgConnHd__ *zpPgConnHd_ = NULL;
    zPgResHd__ *zpPgResHd_ = NULL;

    if (NULL == (zpPgConnHd_ = zPgSQL_.conn(zpConnInfo))) {
        return -90;
    } else {
        if (NULL == (zpPgResHd_ = zPgSQL_.exec(
                        zpPgConnHd_,
                        zpCmd,
                        NULL == zppPgRes_ ? zFalse : zTrue))) {
            zPgSQL_.conn_clear(zpPgConnHd_);
            return -91;
        }

        if (NULL == zppPgRes_) {
            zpg_res_clear(zpPgResHd_, NULL);
        } else if (NULL == (*zppPgRes_ = zPgSQL_.parse_res(zpPgResHd_))) {
            zpg_res_clear(zpPgResHd_, NULL);
            zPgSQL_.conn_clear(zpPgConnHd_);
            return -92;
        } else {
            /* 必须在 parse_res 之后 */
            (*zppPgRes_)->p_pgResHd_ = zpPgResHd_;
        }

        zPgSQL_.conn_clear(zpPgConnHd_);
    }

    return 0;
}


static _i
zpg_exec_with_param_once(char *zpConnInfo, char *zpCmd, _i zParamCnt, const char **zppParam, zPgRes__ **zppPgRes_) {
    zPgConnHd__ *zpPgConnHd_ = NULL;
    zPgResHd__ *zpPgResHd_ = NULL;

    if (NULL == (zpPgConnHd_ = zPgSQL_.conn(zpConnInfo))) {
        return -90;
    } else {
        if (NULL == (zpPgResHd_ = zPgSQL_.exec_with_param(
                        zpPgConnHd_,
                        zpCmd,
                        zParamCnt,
                        zppParam,
                        NULL == zppPgRes_ ? zFalse : zTrue))) {
            zPgSQL_.conn_clear(zpPgConnHd_);
            return -91;
        }

        if (NULL == zppPgRes_) {
            zpg_res_clear(zpPgResHd_, NULL);
        } else if (NULL == (*zppPgRes_ = zPgSQL_.parse_res(zpPgResHd_))) {
            zpg_res_clear(zpPgResHd_, NULL);
            zPgSQL_.conn_clear(zpPgConnHd_);
            return -92;
        } else {
            /* 必须在 parse_res 之后 */
            (*zppPgRes_)->p_pgResHd_ = zpPgResHd_;
        }

        zPgSQL_.conn_clear(zpPgConnHd_);
    }

    return 0;
}


/*
 * 通用的只写动作，无返回内容
 * 原生写入，没有 prepared 优化
 */
static _i
zwrite_db(void *zpCmd) {

    zPgResHd__ *zpHd_;
    _i zKeepID;

    /* 出栈 */
    pthread_mutex_lock(& zDBPoolLock);

    while (0 > zDBPoolStackHeader) {
        pthread_mutex_unlock(& zDBPoolLock);
        sleep(1);
        pthread_mutex_lock(& zDBPoolLock);
    }

    zKeepID = zDBPoolStack[zDBPoolStackHeader];
    zDBPoolStackHeader--;

    pthread_mutex_unlock(& zDBPoolLock);

    /* 若执行失败，记录日志 */
    if (NULL == (zpHd_ = zPgSQL_.exec(zpDBPool_[zKeepID], zpCmd, zFalse))) {
		// write log
    } else {
        zPgSQL_.res_clear(zpHd_, NULL);
    }

    /* 入栈 */
    pthread_mutex_lock(& zDBPoolLock);
    zDBPoolStack[++zDBPoolStackHeader] = zKeepID;
    pthread_mutex_unlock(& zDBPoolLock);

    return 0;
}
