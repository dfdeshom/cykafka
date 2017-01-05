# distutils: language = c++
# cython: profile=True
# cython: linetrace=True
# cython: boundscheck=False
# cython: nonecheck=False
# distutils: define_macros=CYTHON_TRACE_NOGIL=1
from libcpp.string cimport string as cppstring
from libcpp.vector cimport vector
from libcpp.list cimport list as cpplist

cdef extern from "librdkafka/rdkafkacpp.h" namespace "RdKafka":
    cdef enum ErrorCode:
        ERR_NO_ERROR = 0
        ERR__TIMED_OUT = -185
        ERR__PARTITION_EOF = -191
        ERR__UNKNOWN_TOPIC = -188
        ERR__UNKNOWN_PARTITION = -190

    ctypedef enum ConfType_type "RdKafka::Conf::ConfType":
        CONF_GLOBAL "RdKafka::Conf::CONF_GLOBAL"
        CONF_TOPIC "RdKafka::Conf::CONF_TOPIC"

    ctypedef enum ConfResult_type "RdKafka::Conf::ConfResult":
        CONF_UNKNOWN "RdKafka::Conf::CONF_UNKNOWN"
        CONF_INVALID "RdKafka::Conf::CONF_INVALID"
        CONF_OK "RdKafka::Conf::CONF_OK"

    cdef cppclass Conf:
        @staticmethod
        Conf * create(ConfType_type)
        ConfResult_type set(cppstring & , cppstring&, cppstring&)
        ConfResult_type set(cppstring, Conf *, cppstring)
        cpplist[cppstring] * dump()

    cdef cppclass Message:
        cppstring errstr()
        ErrorCode err()
        void * payload()
        size_t len()

    cdef cppclass KafkaConsumer:
        @staticmethod
        KafkaConsumer * create(Conf * , cppstring)
        cppstring name()
        ErrorCode assignment(vector)
        ErrorCode subscribe(vector[cppstring])
        Message * consume(int)
        ErrorCode close()


cdef class Consumer:
    cdef KafkaConsumer * consumer

    def __cinit__(self, *topics, **config_options):
        cdef Conf * conf = Conf.create(ConfType_type.CONF_GLOBAL)
        cdef Conf * topic_conf = Conf.create(ConfType_type.CONF_TOPIC)
        cdef conf_res
        cdef cppstring errstr
        cdef ErrorCode err_code
        cdef cppstring opt
        # TODO: handle specila types like callbacks,
        # events, default topic config, etc

        # try setting global configs first, then fall through
        # to topic conf if that fails
        for option, value in config_options.iteritems():
            opt = <cppstring > option
            conf_res = conf.set(< cppstring & > option, < cppstring & > value, errstr)
            # try topic conf
            if conf_res != ConfResult_type.CONF_OK:
                conf_res = topic_conf.set(opt, < cppstring > value, errstr)
            if conf_res != ConfResult_type.CONF_OK:
                raise Exception("%s: (%s,%s)" % (errstr, option, value))

        # set configure topic
        conf.set(< cppstring & > "default_topic_conf",
                  < Conf * > topic_conf,
                  errstr)
        del topic_conf

        self.consumer = KafkaConsumer.create(conf, errstr)
        if not self.consumer:
            # print "Error!"
            # print errstr
            pass

        del conf

        # subscribe to topic
        err_code = self.consumer.subscribe(topics)
        if err_code:
            # print "Error!"
            # print err_code
            pass
        # print 'done w/ init'

    def __dealloc__(self):
        del self.consumer

    def close(self):
        self.consumer.close()

    cdef tuple handle_message(self, Message * message):
        cdef int error_code
        cdef char * msg_ptr

        error_code = message.err()
        # print error_code
        if error_code == ErrorCode.ERR_NO_ERROR:
            msg_ptr = <char * >message.payload()
            res = msg_ptr[:message.len()]
            res = res.strip()
            return error_code, res

        return error_code, message.errstr()

    def consume(self):
        # print 'created client with name:', self.consumer.name()
        cdef int err
        cdef Message * resp
        cdef tuple handle_result
        while True:
            resp = self.consumer.consume(100)
            handle_result = self.handle_message(resp)
            err = handle_result[0]
            msg = handle_result[1]
            #err, msg = self.handle_message(resp)
            del resp
            # print 'message: ', msg
            if err == ERR_NO_ERROR:
                if msg:
                    yield msg
            # print msg