#include <algorithm>
#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/queue.h>
#include <sys/types.h>
#include <fstream>
#include <sstream>
#include <set>
#include <iostream>
#include <getopt.h>
#include <assert.h>
#include <signal.h>

#include <cstring>
#include <thread>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "sai_rpc.h"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdint.h>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

#define UNREFERENCED_PARAMETER(P)   (P)

extern "C" {
#include "sai.h"
#include "saistatus.h"
#include "switch_sai_rpc_server.h"
int start_sai_thrift_rpc_server(int port);
}

#define SWITCH_SAI_THRIFT_RPC_SERVER_PORT 9092
#define MODEL_PORT 46500
#define MODEL_IP "127.0.0.1"

sai_switch_api_t* sai_switch_api;
static int _model_socket;

std::map<std::string, std::string> gProfileMap;
std::map<std::set<int>, std::string> gPortMap;

std::vector<std::pair<sai_fdb_entry_t, sai_object_id_t>> gFdbMap;

sai_object_id_t gSwitchId; ///< SAI switch global object ID.

class InsertRequest {
public:
    class Value {
    public:
        class Ternary {
        public:
            std::string value;
            std::string mask;
        };
        class LPM {
        public:
            std::string value;
            int prefix_len;
        };
        class Range {
        public:
            std::string first;
            std::string last;
        };

        std::string exact;
        Ternary ternary;
        LPM prefix;
        Range range;
        std::vector<Ternary> ternary_list;
        std::vector<Range> range_list;
    };

    int table;
    std::vector<Value> values;
    int action;
    std::vector<std::string> params;
    int priority;

    std::string jsonize() {
        rapidjson::StringBuffer s;
        rapidjson::Writer<rapidjson::StringBuffer> writer(s);

        writer.StartObject();
            writer.Key("table");
            writer.Uint(table);

            writer.Key("values");
            writer.StartArray();
            for (auto & v : values) {
                writer.StartObject();
                    writer.Key("exact");
                    writer.String(v.exact.c_str());

                    writer.Key("ternary");
                    writer.StartObject();
                        writer.Key("value");
                        writer.String(v.ternary.value.c_str());

                        writer.Key("mask");
                        writer.String(v.ternary.mask.c_str());
                    writer.EndObject();

                    writer.Key("prefix");
                    writer.StartObject();
                        writer.Key("value");
                        writer.String(v.prefix.value.c_str());

                        writer.Key("prefix_len");
                        writer.Uint(v.prefix.prefix_len);
                    writer.EndObject();

                    writer.Key("range");
                    writer.StartObject();
                        writer.Key("first");
                        writer.String(v.range.first.c_str());

                        writer.Key("last");
                        writer.String(v.range.last.c_str());
                    writer.EndObject();

                    writer.Key("ternary_list");
                    writer.StartArray();
                    for (auto & item : v.ternary_list) {
                        writer.StartObject();
                            writer.Key("value");
                            writer.String(item.value.c_str());

                            writer.Key("mask");
                            writer.String(item.mask.c_str());
                        writer.EndObject();
                    }
                    writer.EndArray();

                    writer.Key("range_list");
                    writer.StartArray();
                    for (auto & item : v.range_list) {
                        writer.StartObject();
                            writer.Key("first");
                            writer.String(item.first.c_str());

                            writer.Key("last");
                            writer.String(item.last.c_str());
                        writer.EndObject();
                    }
                    writer.EndArray();
                writer.EndObject();
            }
            writer.EndArray();

            writer.Key("action");
            writer.Uint(action);

            writer.Key("params");
            writer.StartArray();
            for (auto & p : params) {
                writer.String(p.c_str());
            }
            writer.EndArray();

            writer.Key("priority");
            writer.Uint(priority);
        writer.EndObject();

        const char * buf = s.GetString();
        int buf_size = s.GetSize();
        return std::string(buf, buf_size);
    }
};

void on_switch_state_change(_In_ sai_object_id_t switch_id,
                            _In_ sai_switch_oper_status_t switch_oper_status)//
{
}

void model_api_insert(InsertRequest& insertRequest) {
    uint8_t api_id = 0;
    uint32_t json_buf_size;
    const char *json_buf;
    char json_buf_size_cstr[16];
    bool status;

    send(_model_socket, &api_id, sizeof(uint8_t), 0);

    std::string json_repr = insertRequest.jsonize();
    json_buf = json_repr.c_str();
    json_buf_size = strlen(json_buf);

    snprintf(json_buf_size_cstr, 16, "%08X", json_buf_size);
    send(_model_socket, json_buf_size_cstr, 8, 0);

    send(_model_socket, json_buf, json_buf_size, 0);

    read(_model_socket, &status, 1);
}

void on_fdb_event(_In_ uint32_t count,
                  _In_ sai_fdb_event_notification_data_t *data)
{
    sai_fdb_event_t event_type;
    sai_fdb_entry_t fdb_entry;
    uint32_t attr_count;
    sai_attribute_t *attr;
    sai_object_id_t bv_id;
    sai_object_id_t bport_id = 0;

    attr = data->attr;
    event_type = data->event_type;
    fdb_entry = data->fdb_entry;
    bv_id = fdb_entry.bv_id;
    attr_count = data->attr_count;

    for (uint32_t i = 0; i < attr_count; i++)
    {
        if (attr[i].id == SAI_FDB_ENTRY_ATTR_BRIDGE_PORT_ID)
            bport_id = attr[i].value.oid;
    }

    InsertRequest insertRequest;
    insertRequest.table = 19;  // Example table ID

    InsertRequest::Value fdb_value;
    fdb_value.exact = std::string(reinterpret_cast<const char *>(fdb_entry.mac_address), 6); // Use MAC as exact match

    // Handle different FDB event types
    switch (event_type)
    {
    case SAI_FDB_EVENT_LEARNED:
        fdb_value.ternary.value = std::to_string(bv_id);
        fdb_value.ternary.mask = std::to_string(bport_id);
        insertRequest.values.push_back(fdb_value);
        insertRequest.action = 20; // Example action ID for "LEARNED"
        model_api_insert(insertRequest);
        break;

    case SAI_FDB_EVENT_FLUSHED:
        // Example: Send a flush event to the model
        insertRequest.action = 21; // Example action ID for "FLUSHED"
        model_api_insert(insertRequest);
        break;

    case SAI_FDB_EVENT_MOVE:
        fdb_value.ternary.value = std::to_string(bv_id);
        fdb_value.ternary.mask = std::to_string(bport_id);
        insertRequest.values.push_back(fdb_value);
        insertRequest.action = 22; // Example action ID for "MOVE"
        model_api_insert(insertRequest);
        break;

    case SAI_FDB_EVENT_AGED:
        fdb_value.ternary.value = std::to_string(bv_id);
        fdb_value.ternary.mask = std::to_string(bport_id);
        insertRequest.values.push_back(fdb_value);
        insertRequest.action = 23; // Example action ID for "AGED"
        model_api_insert(insertRequest);
        break;

    default:
        printf("Unknown FDB event type\n");
        break;
    }
}  

void on_port_state_change(_In_ uint32_t count,
                          _In_ sai_port_oper_status_notification_t *data)
{
}

void on_shutdown_request(_In_ sai_object_id_t switch_id)//
{
}

void on_packet_event(_In_ sai_object_id_t switch_id,
                     _In_ const void *buffer,
                     _In_ sai_size_t buffer_size,
                     _In_ uint32_t attr_count,
                     _In_ const sai_attribute_t *attr_list)
{
}

// Profile services
/* Get variable value given its name */
const char* test_profile_get_value(
        _In_ sai_switch_profile_id_t profile_id,
        _In_ const char* variable)
{
    UNREFERENCED_PARAMETER(profile_id);

    if (variable == NULL)
    {
        printf("variable is null\n");
        return NULL;
    }

    std::map<std::string, std::string>::const_iterator it = gProfileMap.find(variable);
    if (it == gProfileMap.end())
    {
        printf("%s: NULL\n", variable);
        return NULL;
    }

    return it->second.c_str();
}

std::map<std::string, std::string>::iterator gProfileIter = gProfileMap.begin();
/* Enumerate all the K/V pairs in a profile.
   Pointer to NULL passed as variable restarts enumeration.
   Function returns 0 if next value exists, -1 at the end of the list. */
int test_profile_get_next_value(
        _In_ sai_switch_profile_id_t profile_id,
        _Out_ const char** variable,
        _Out_ const char** value)
{
    UNREFERENCED_PARAMETER(profile_id);

    if (value == NULL)
    {
        printf("resetting profile map iterator");

        gProfileIter = gProfileMap.begin();
        return 0;
    }

    if (variable == NULL)
    {
        printf("variable is null");
        return -1;
    }

    if (gProfileIter == gProfileMap.end())
    {
        printf("iterator reached end");
        return -1;
    }

    *variable = gProfileIter->first.c_str();
    *value = gProfileIter->second.c_str();

    printf("key: %s:%s", *variable, *value);

    gProfileIter++;

    return 0;
}

const sai_service_method_table_t test_services = {
    test_profile_get_value,
    test_profile_get_next_value
};

struct cmdOptions
{
    std::string profileMapFile;
    std::string portMapFile;
    std::string initScript;
};

cmdOptions handleCmdLine(int argc, char **argv)
{

    cmdOptions options = {};

    while(true)
    {
        static struct option long_options[] =
        {
            { "profile",          required_argument, 0, 'p' },
            { "portmap",          required_argument, 0, 'f' },
            { "init-script",      required_argument, 0, 'S' },
            { 0,                  0,                 0,  0  }
        };

        int option_index = 0;

        int c = getopt_long(argc, argv, "p:f:S:", long_options, &option_index);

        if (c == -1)
            break;

        switch (c)
        {
            case 'p':
                printf("profile map file: %s\n", optarg);
                options.profileMapFile = std::string(optarg);
                break;

            case 'f':
                printf("port map file: %s\n", optarg);
                options.portMapFile = std::string(optarg);
                break;

            case 'S':
                printf("init script: %s\n", optarg);
                options.initScript = std::string(optarg);
                break;

            default:
                printf("getopt_long failure\n");
                exit(EXIT_FAILURE);
        }
    }

    return options;
}

void handleProfileMap(const std::string& profileMapFile)
{

    if (profileMapFile.size() == 0)
        return;

    std::ifstream profile(profileMapFile);

    if (!profile.is_open())
    {
        printf("failed to open profile map file: %s : %s\n", profileMapFile.c_str(), strerror(errno));
        exit(EXIT_FAILURE);
    }

    std::string line;

    while(getline(profile, line))
    {
        if (line.size() > 0 && (line[0] == '#' || line[0] == ';'))
            continue;

        size_t pos = line.find("=");

        if (pos == std::string::npos)
        {
            printf("not found '=' in line %s\n", line.c_str());
            continue;
        }

        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);

        gProfileMap[key] = value;

        printf("insert: %s:%s\n", key.c_str(), value.c_str());
    }
}

void handlePortMap(const std::string& portMapFile)
{

    if (portMapFile.size() == 0)
        return;

    std::ifstream portmap(portMapFile);

    if (!portmap.is_open())
    {
        printf("failed to open port map file: %s : %s\n", portMapFile.c_str(), strerror(errno));
        exit(EXIT_FAILURE);
    }

    std::string line;

    while(getline(portmap, line))
    {
        if (line.size() > 0 && (line[0] == '#' || line[0] == ';'))
            continue;

        size_t pos = line.find("=");

        if (pos == std::string::npos)
        {
            printf("not found '=' in line %s\n", line.c_str());
            continue;
        }

        std::string key = line.substr(0, pos);
        std::string value = line.substr(pos + 1);

        std::set<int> portSet;
        std::stringstream ss(key);
        int port;

        while (ss >> port)
        {
            portSet.insert(port);
            if (ss.peek() == ',')
                ss.ignore();
        }

        gPortMap[portSet] = value;

        printf("insert: ");
        for (const auto& p : portSet)
        {
            printf("%d,", p);
        }
        printf(":%s\n", value.c_str());
    }
}

void handleInitScript(const std::string& initScript)
{

    if (initScript.size() == 0)
        return;

    printf("Running %s ...\n", initScript.c_str());
    system(initScript.c_str());
}


int model_api_init() {
    struct sockaddr_in serv_addr;
    if ((_model_socket = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        std::cout<<"Could not create socket"<<std::endl;
        return -1;
    }
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(MODEL_PORT);
    inet_pton(AF_INET, MODEL_IP, &serv_addr.sin_addr);
    if (connect(_model_socket, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
        std::cout<<"Could not connect to server"<<std::endl;
        return -1;
    }
    return 0;
}

int main(int argc, char **argv)
{
    cmdOptions options = handleCmdLine(argc, argv);

    handleProfileMap(options.profileMapFile);
    handlePortMap(options.portMapFile);

    sai_api_initialize(0, (sai_service_method_table_t *)&test_services);

    sai_api_query(SAI_API_SWITCH, (void **)&sai_switch_api);

    sai_attribute_t attr;
    attr.id = SAI_SWITCH_ATTR_INIT_SWITCH;
    attr.value.booldata = true;

    // sai_status_t status = sai_switch_api->create_switch(&gSwitchId, 0, 1, &attr);
    // if (status != SAI_STATUS_SUCCESS)
    // {
    //     printf("Failed to create switch: %d\n", status);
    //     return EXIT_FAILURE;
    // }

    //sai_switch_api->set_switch_attribute(gSwitchId, &attr);

    start_sai_thrift_rpc_server(SWITCH_SAI_THRIFT_RPC_SERVER_PORT);
    // if (status != 0)
    // {
    //     printf("Failed to start SAI Thrift RPC server\n");
    //     return EXIT_FAILURE;
    // }

    printf("SAI Thrift RPC server started on port %d\n", SWITCH_SAI_THRIFT_RPC_SERVER_PORT);

    _model_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (_model_socket < 0)
    {
        perror("Socket creation failed");
        return EXIT_FAILURE;
    }

    struct sockaddr_in model_addr;
    model_addr.sin_family = AF_INET;
    model_addr.sin_port = htons(MODEL_PORT);
    inet_pton(AF_INET, MODEL_IP, &model_addr.sin_addr);

    if (connect(_model_socket, (struct sockaddr *)&model_addr, sizeof(model_addr)) < 0)
    {
        perror("Failed to connect to model API");
        return EXIT_FAILURE;
    }

    printf("Connected to model API at %s:%d\n", MODEL_IP, MODEL_PORT);

    // Main program loop or thread handling logic can go here.

    // Cleanup and shutdown
    on_fdb_event();
    close(_model_socket);
    sai_api_uninitialize();

    while (1) pause();

    return 0;
}