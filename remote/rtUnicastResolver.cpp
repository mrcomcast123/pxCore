#include "rtRemote.h"
#include "rtRemoteEnvironment.h"
#include "rtRemoteMulticastResolver.h"
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <uWS.h>
#include <list>
#include <queue>

#define WEBSOCKET_PORT_NUMBER 3000  //TODO - add to config

using namespace uWS;
using namespace std;

typedef WebSocket<SERVER> WSSocket;
class Resolver;

class Daemon
{
public:
    Daemon();
    ~Daemon();    
};

class WorkerThread
{
 public:
    WorkerThread(Resolver& r);
    ~WorkerThread();
    void run();
    void search(WSSocket* socket);
    void exit();
    bool idle();
  private:
    enum State { StateIdle, StateSearch, StateExit };
    Resolver& m_r;
    rtRemoteMulticastResolver* m_mr;
    State m_state;
    WSSocket* m_socket;
    thread m_thread;
    mutex m_mutex;
    condition_variable m_cv;
};

class Resolver
{
public:
    void run();
    rtRemoteMulticastResolver* createMulticastResolver();
    void searchMulticastResolver(WSSocket* socket, rtRemoteMulticastResolver* mr);
    WSSocket* dequeue();
private:
    void enqueue(WSSocket* socket);
    static const int MAX_THREADS = 4;
    vector<WorkerThread*> m_threads;
    queue<WSSocket*> m_waitQueue;
    mutex m_queueMutex;
    rtRemoteEnvironment* m_env;
    sockaddr_storage m_rpc;
    pid_t m_pid;
    Hub m_hub;
};

WorkerThread::WorkerThread(Resolver& r) : 
    m_r(r), 
    m_state(StateIdle),
    m_thread(&WorkerThread::run, this)
{
    m_mr = m_r.createMulticastResolver();
}

WorkerThread::~WorkerThread()
{
    delete m_mr;
}

void WorkerThread::run()
{
    rtLogInfo("WorkerThread %d run enter\n", (int)m_thread.native_handle());
    for(;;)
    {
        State state;
        {
            unique_lock<mutex> lk(m_mutex);
            m_cv.wait(lk, [this]{return m_state;});
            state = m_state;
        }
       
        if(state == StateExit)
            break;
      
        if(state == StateSearch)
        {
            rtLogInfo("WorkerThread %d search: %s\n", (int)m_thread.native_handle(), (char*)m_socket->getUserData());

            m_r.searchMulticastResolver(m_socket, m_mr);

            rtLogInfo("WorkerThread %d finished search: %s\n", (int)m_thread.native_handle(), (char*)m_socket->getUserData());

            delete (char*)m_socket->getUserData();
            
            m_socket->close();//force a disconnect (note client will not get a response if object not found)

            {
                unique_lock<mutex> lk(m_mutex);
                m_socket = m_r.dequeue();
                if(!m_socket)
                    m_state = StateIdle;
            }
        }
    }
    rtLogInfo("WorkerThread %d run exit\n", (int)m_thread.native_handle());
}

void WorkerThread::search(WSSocket* socket)
{
    lock_guard<mutex> lk(m_mutex);
    m_state = StateSearch;
    m_socket = socket;
    m_cv.notify_one();
}

void WorkerThread::exit()
{
    {
        lock_guard<mutex> lk(m_mutex);
        m_state = StateExit;
        m_cv.notify_one();
    }
    m_thread.join();
}

bool WorkerThread::idle()
{
    lock_guard<mutex> lk(m_mutex);
    return m_state == StateIdle;
}

void Resolver::run()
{
    m_pid = getpid();
    m_env = rtEnvironmentGetGlobal();
    rtError e = rtRemoteInit(m_env);
    if (e != RT_OK)
    {
        rtLogError("rtRemoteInit:%s", rtStrError(e));
        ::exit(EXIT_FAILURE);
    }
    memset(&m_rpc, 0, sizeof(m_rpc));
    
    if (true)//TODO fix this strcmp crash if(!strcmp(m_env->Config->server_socket_family().c_str(), "unix"))
    {
        char path[UNIX_PATH_MAX];
        memset(path, 0, sizeof(path));
        rtError e = rtCreateUnixSocketName(0, path, sizeof(path));
        if (e != RT_OK)
        {
            rtLogError("rtCreateUnixSocketName failed %s. %s", path, rtStrError(e));
            ::exit(EXIT_FAILURE);
        }

        int ret = unlink(path); // reuse path if needed
        if (ret == -1 && errno != ENOENT)
        {
            rtError e = rtErrorFromErrno(errno);
            rtLogInfo("error trying to remove %s. %s", path, rtStrError(e));
            ::exit(EXIT_FAILURE);
        }

        struct sockaddr_un *unAddr = reinterpret_cast<sockaddr_un*>(&m_rpc);
        unAddr->sun_family = AF_UNIX;
        strncpy(unAddr->sun_path, path, UNIX_PATH_MAX);
    }
    else
    {
        rtGetDefaultInterface(m_rpc, 0);
    }

    m_hub.onConnection([](WebSocket<SERVER> *ws, HttpRequest req) {
        rtLogInfo("client connected\n");
    });

    m_hub.onDisconnection([](WebSocket<SERVER> *ws, int code, char *message, size_t length) {
        rtLogInfo("client disconnected\n");
    });

    m_hub.onMessage([this](WebSocket<SERVER> *ws, char *message, size_t length, OpCode opCode) {
        //ws->send("hello", 5, opCode);
        char* s = new char[length+1];
        memcpy(s, message, length);
        s[length]=0;
        ws->setUserData(s);
        this->enqueue(ws);
    });

    if(!m_hub.listen(WEBSOCKET_PORT_NUMBER))
    {
        rtLogError("uWebSocket Hub failed to listen on port\n");
        ::exit(EXIT_FAILURE);
    }

    m_hub.run();
}

rtRemoteMulticastResolver* Resolver::createMulticastResolver()
{
    rtRemoteMulticastResolver* r = new rtRemoteMulticastResolver(m_env);
    rtError e = r->open(m_rpc);
    if (e != RT_OK)
    {
        rtLogWarn("WorkerThread failed to open rtRemoteMulticastResolver %s", rtStrError(e));
        ::exit(EXIT_FAILURE);
    }
    return r;
}

void Resolver::searchMulticastResolver(WSSocket* socket, rtRemoteMulticastResolver* mr)
{
    char* message = (char*)socket->getUserData();
    rtRemoteMessagePtr doc;
    rtError err = rtParseMessage(message, strlen(message), doc);
    if (err == RT_OK)
    {
        char const* messageType = rtMessage_GetMessageType(*doc);
        char const* objectId = rtMessage_GetObjectId(*doc);
        string corrKey = rtMessage_GetCorrelationKey(*doc).toString();
        if( messageType && strcmp(messageType, kMessageTypeSearch) == 0 && objectId && !corrKey.empty() )
        {
            rtLogInfo("Resolver searching for object '%s'\n", objectId);
            rtError err = RT_OK;
            sockaddr_storage objectEndpoint;
            err = mr->locateObject(objectId, objectEndpoint, WEBSOCKET_PORT_NUMBER);
            if (err == RT_OK)
            {
                string endPoint = rtSocketToString(objectEndpoint);
                rtLogInfo("Resolver located '%s' endpoint '%s'\n", objectId, endPoint.c_str());

                rapidjson::Document doc;
                doc.SetObject();
                doc.AddMember(kFieldNameMessageType, kMessageTypeLocate, doc.GetAllocator());
                doc.AddMember(kFieldNameObjectId, string(objectId), doc.GetAllocator());
                doc.AddMember(kFieldNameEndPoint, endPoint, doc.GetAllocator());
                doc.AddMember(kFieldNameSenderId, m_pid, doc.GetAllocator());
                doc.AddMember(kFieldNameCorrelationKey, corrKey, doc.GetAllocator());

                rapidjson::StringBuffer buff;
                rapidjson::Writer<rapidjson::StringBuffer> writer(buff);
                doc.Accept(writer);
                rtLogInfo("Resolver response: %s\n", buff.GetString());
                socket->send(buff.GetString());
            }
            else
            {
                rtLogWarn("Resolver failed to find object '%s'\n", objectId);
            }
        }
        else
        {
            rtLogWarn("Resolver failed to parse '%s' message.\n", kMessageTypeSearch);
        }
    }
    else
    {
        rtLogWarn("Resolver failed to parse json err=%d\n", (int)err);
    }
}

void Resolver::enqueue(WSSocket* socket)
{
    WorkerThread* thread = nullptr;
    for(int i = 0; i < (int)m_threads.size(); ++i)
    {
       if(m_threads[i]->idle())
           thread = m_threads[i];
    }
    if(thread == nullptr && m_threads.size() < MAX_THREADS)
    {
        thread = new WorkerThread(*this);
        m_threads.push_back(thread);
    }
    if(thread)
    {
         thread->search(socket);
    }
    else
    {
        lock_guard<mutex> lk(m_queueMutex);
        m_waitQueue.push(socket);
    }
}

WSSocket* Resolver::dequeue()
{
    WSSocket* socket = nullptr;
    lock_guard<mutex> lk(m_queueMutex);
    if(!m_waitQueue.empty())
    {
        socket = m_waitQueue.front();
        m_waitQueue.pop();
    }
    return socket;
}

Daemon::Daemon()
{
    pid_t pid, sid;
    pid = fork();
    if(pid > 0)
        exit(EXIT_SUCCESS);
    else
        exit(EXIT_FAILURE);
    umask(0);
    openlog("rtUnicastResolver", LOG_NOWAIT | LOG_PID, LOG_USER);
    syslog(LOG_NOTICE, "Successfully started daemon-name");
    sid = setsid();
    if(sid < 0)
    {
        syslog(LOG_ERR, "Could not generate session ID for child process");
        exit(EXIT_FAILURE);
    }
    if((chdir("/")) < 0)
    {
        syslog(LOG_ERR, "Could not change working directory to /");
        exit(EXIT_FAILURE);
    }
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
}

Daemon::~Daemon()
{
   syslog(LOG_NOTICE, "Stopping daemon-name");
   closelog();
}

int main()
{
    Daemon d;
    Resolver r;
    r.run();
    return 1;
}


