/*
* Copyright Hanwei King
*/

#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>

#include <librdkafka/rdkafka.h>

#define MAX_BODY_LENGTH 1048576     // the max accept message size

static void* ngx_http_growing_api_create_main_conf(ngx_conf_t *cf);
static void* ngx_http_growing_api_create_loc_conf(ngx_conf_t *cf);
static char* ngx_conf_set_growing_api_loc(ngx_conf_t *cf, ngx_command_t *cmd, void *conf);

static void* ngx_http_growing_api_init_worker(ngx_cycle_t *cycle);
static void* ngx_http_growing_api_exit_worker(ngx_cycle_t *cycle);
static void* ngx_http_growing_api_producer_callback_handler(rd_kafka_t *rk,
	void *payload, size_t len, int error_code, void *opaque, void *msg_opaque);

// request body handler
static ngx_int_t ngx_http_growing_api_handler(ngx_http_request_t *r);
static void  ngx_http_growing_api_body_handler(ngx_http_request_t *r);
static void ngx_http_growing_api_send_response(ngx_http_request_t *r);

typedef struct {
    // configure value
    ngx_str_t               *brokers;
    // init value
    rd_kafka_t              *kafka;
    rd_kafka_conf_t         *kafka_conf;
} ngx_http_growing_api_main_conf_t;


typedef struct {
    // configure value
    ngx_str_t                topic;
    rd_kafka_topic_t        *kafka_topic;
    rd_kafka_topic_conf_t   *kafka_topic_conf;
} ngx_http_growing_api_loc_conf_t;


static ngx_command_t ngx_http_growing_api_commands[] = {
    {
        ngx_string("kafka_brokers"),
        NGX_HTTP_MAIN_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_str_slot,
        NGX_HTTP_MAIN_CONF_OFFSET,
        offsetof(ngx_http_growing_api_main_conf_t, brokers),
        NULL
    },
    {
        ngx_string("kafka_topic"),
        NGX_HTTP_LOC_CONF|NGX_CONF_TAKE1,
        ngx_conf_set_growing_api_loc,
        NGX_HTTP_LOC_CONF_OFFSET,
        offsetof(ngx_http_growing_api_loc_conf_t, topic),
        NULL
    },
    ngx_null_command
};

static ngx_http_module_t ngx_http_growing_api_module_ctx {
    NULL,
    NULL,
    ngx_http_growing_api_create_main_conf,
    NULL,
    NULL,
    NULL,
    ngx_http_growing_api_create_loc_conf,
    NULL,
};

ngx_module_t ngx_http_growing_api_module = {
    NGX_MODULE_V1,
    &ngx_http_growing_api_module_ctx,
    ngx_http_growing_api_commands,
    NGX_HTTP_MODULE,
    NULL,
    NULL,
    ngx_http_growing_api_init_worker,
    NULL,
    NULL,
    ngx_http_growing_api_exit_worker,
    NULL,
    NGX_MODULE_V1_PADDING
};

static void *ngx_http_growing_api_create_main_conf(ngx_conf_t *cf) {
    ngx_http_growing_api_main_conf_t *main_conf;
    main_conf = (ngx_http_growing_api_main_conf_t *)ngx_pcalloc(cf->pool,
        sizeof(ngx_http_growing_api_main_conf_t));
    if (main_conf == NULL) {
        return NGX_CONF_ERROR;
    }

    main_conf->kafka = NULL;
    main_conf->kafka_conf = NULL;
    ngx_str_null(&main_conf->brokers);

    return main_conf;
}

static void *ngx_http_growing_api_create_loc_conf(ngx_conf_t *cf) {
    ngx_http_growing_api_loc_conf_t *loc_conf;
    loc_conf = (ngx_http_growing_api_loc_conf_t *)ngx_pcalloc(cf->pool,
        sizeof(ngx_http_growing_api_loc_conf_t));
    if (loc_conf == NULL) {
        return NGX_CONF_ERROR;
    }

    loc_conf->kafka_topic = NULL;
    loc_conf->kafka_topic_conf = NULL;
    ngx_str_null(&loc_conf->topic);

    return loc_conf;
}

static char* ngx_conf_set_growing_api_loc(ngx_conf_t *cf, ngx_command_t *cmd, void *conf) {
    ngx_http_core_loc_conf_t *clcf;
    ngx_http_growing_api_main_conf_t *main_conf;
    ngx_http_growing_api_loc_conf_t *loc_conf = conf;

    clcf = ngx_http_conf_get_module_loc_conf(cf, ngx_http_core_module);
    if (clcf == NULL) {
        return NGX_CONF_ERROR;
    }
    clcf->handler = ngx_http_growing_api_handler;

    if (cf->args->nelts != 1) {
        return NGX_CONF_ERROR;
    }

    loc_conf->topic = args->args->elts[1];

    loc_conf->kafka_topic_conf = rd_kafka_topic_conf_new();
    // get kafka object in main conf
    main_conf = ngx_http_conf_get_module_main_conf(ngx_http_growing_api_module);
    // configure kafka topic to send
    loc_conf->topic = rd_kafka_topic_new(main_conf->kafka,
        (const char *)local_conf->topic.data, loc_conf->kafka_topic_conf);

    return NGX_CONF_OK;
}

static void* ngx_http_growing_api_producer_callback_handler(
    rd_kafka_t *rk,
	void *payload,
    size_t len,
    int error_code,
    void *opaque,
    void *msg_opaque) {
    if (error_code != 0) {
        ngx_log_error(NGX_LOG_ERR, (ngx_log_t *)msg_opaque, 0, rd_kafka_err2str(error_code));
    }
}

static void *ngx_http_growing_api_init_worker(ngx_cycle_t *cycle) {
    ngx_http_growing_api_main_conf_t *main_conf;

    main_conf = ngx_http_cycle_get_module_main_conf(cycle,
        ngx_http_growing_api_module);

    main_conf->kafka_conf = rd_kafka_conf_new();
    rd_kafka_conf_set_dr_cb(main_conf->kafka_conf,
        ngx_http_growing_api_producer_callback_handler);
    main_conf->kafka = rd_kafka_new(RD_KAFKA_PRODUCER, main_conf->kafka_conf, NULL, 0);

    rd_kafka_brokers_add(main_conf->kafka, (const char *)main_conf->brokers.data);
}

static void* ngx_http_growing_api_exit_worker(ngx_cycle_t *cycle) {
    ngx_http_growing_api_main_conf_t *main_conf;

    main_conf = ngx_http_cycle_get_module_main_conf(cycle,
        ngx_http_growing_api_module);

    rd_kafka_destroy(main_conf->kafka);
}


static ngx_int_t ngx_http_growing_api_handler(ngx_http_request_t *r) {
    if (!(r->method & (NGX_HTTP_POST))) {
        return NGX_HTTP_NOT_ALLOWED;
    }

    if (r->headers_in.content_length_n >= (off_t)MAX_BODY_LENGTH) {
        // discard body
        ngx_int_t rc = ngx_http_discard_request_body(r);
        if (rc != NGX_OK) {
            return rc;
        }
        ngx_http_finalize_request(r, NGX_HTTP_REQUEST_ENTITY_TOO_LARGE);

    } else {    // accept the body
        rv = ngx_http_read_client_request_body(r, ngx_http_growing_api_body_handler);
        if (rv >= NGX_HTTP_SPECIAL_RESPONSE) {
            return rv;
        }

        return NGX_DONE;
    }
}

static void  ngx_http_growing_api_body_handler(ngx_http_request_t *r) {
    int nbufs;
    u_char *msg;
    size_t len;
    ngx_chain_t *cl, *in;
    ngx_http_request_body_t *body;
    ngx_http_growing_api_loc_conf_t *loc_conf;

    loc_conf = ngx_http_get_module_loc_conf(r, ngx_http_growing_api_module);

    /* get body */
    body = r->request_body;
    if (body == NULL || body->bufs == NULL) {
        ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
        return;
    }

    main_conf = NULL;

    /* calc len and bufs */
    len = 0;
    nbufs = 0;
    in = body->bufs;
    for (cl = in; cl != NULL; cl = cl->next) {
        nbufs++;
        len += (size_t)(cl->buf->last - cl->buf->pos);
    }

    /* get msg */
    if (nbufs == 0) {
        goto end;
    }

    if (nbufs == 1 && ngx_buf_in_memory(in->buf)) {
        msg = in->buf->pos;
    } else {
        if ((msg = ngx_pnalloc(r->pool, len)) == NULL) {
            ngx_http_finalize_request(r, NGX_HTTP_INTERNAL_SERVER_ERROR);
            return;
        }

        for (cl = in; cl != NULL; cl = cl->next) {
            if (ngx_buf_in_memory(cl->buf)) {
                msg = ngx_copy(msg, cl->buf->pos, cl->buf->last - cl->buf->pos);
            } else {
                /* TODO: handle buf in file */
                ngx_log_error(NGX_LOG_NOTICE, r->connection->log, 0,
                        "ngx_http_kafka_handler cannot handler in-file-post-buf");
                ngx_http_finalize_request(r, NGX_HTTP_REQUEST_ENTITY_TOO_LARGE);
                return;
            }
        }
        msg -= len;
    }

    rd_kafka_produce(
        loc_conf->kafka_topic,
        RD_KAFKA_PARTITION_UA, // replace with session which could get from header
        RD_KAFKA_MSG_F_COPY,
        (void *)msg, len,
        NULL, 0, loc_conf->log);

    r->headers_out.status = NGX_HTTP_OK;
    ngx_http_growing_api_send_response(r);
}

static void ngx_http_growing_api_send_response(ngx_http_request_t *r) {
    ngx_http_send_header(r);
    ngx_http_finalize_request(r, NGX_OK);
    if (loc_conf != NULL) {
        rd_kafka_poll(loc_conf->kafka, 0);
    }
}
