/**
 * Copyright (c) 2023 Microsoft Open Technologies, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License"); you may
 *    not use this file except in compliance with the License. You may obtain
 *    a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 *    THIS CODE IS PROVIDED ON AN *AS IS* BASIS, WITHOUT WARRANTIES OR
 *    CONDITIONS OF ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT
 *    LIMITATION ANY IMPLIED WARRANTIES OR CONDITIONS OF TITLE, FITNESS
 *    FOR A PARTICULAR PURPOSE, MERCHANTABILITY OR NON-INFRINGEMENT.
 *
 *    See the Apache Version 2.0 License for specific language governing
 *    permissions and limitations under the License.
 *
 *    Microsoft would like to thank the following companies for their review and
 *    assistance with these files: Intel Corporation, Mellanox Technologies Ltd,
 *    Dell Products, L.P., Facebook, Inc., Marvell International Ltd.
 *
 * @file    sai_rpc_frontend.main.cpp
 *
 * @brief   This module contains SAI RPC main function just for linkage test
 */

extern "C" {
#include "sai.h"
int start_sai_thrift_rpc_server(int port);
}

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

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

#include <stdio.h>
#include <dlfcn.h>  // For dynamic loading
#include "saimetadata.h"
// this is just dummy frontend main, just to see if sai_rpc_frontend.cpp will
// compile and link successfully

#define SWITCH_SAI_THRIFT_RPC_SERVER_PORT 9092
#define API_IMPLEMENTED_TEST 1

sai_switch_api_t* sai_switch_api;

// Assuming you have a global pointer to sai_global_apis_t
sai_global_apis_t *global_apis;

std::map<std::string, std::string> gProfileMap;
std::map<std::set<int>, std::string> gPortMap;

std::vector<std::pair<sai_fdb_entry_t, sai_object_id_t>> gFdbMap;

sai_object_id_t gSwitchId;

void on_switch_state_change(_In_ sai_object_id_t switch_id,
                            _In_ sai_switch_oper_status_t switch_oper_status)//
{
}

void on_fdb_event(_In_ uint32_t count,
                  _In_ sai_fdb_event_notification_data_t *data)
{
    sai_fdb_event_t event_type;
    sai_fdb_entry_t fdb_entry;
    uint32_t attr_count;
    sai_attribute_t *attr;
    sai_object_id_t bv_id;
    sai_object_id_t bport_id;
    
    attr = data->attr;
    event_type = data->event_type;
    fdb_entry = data->fdb_entry;
    bv_id = fdb_entry.bv_id;
    attr_count = data ->attr_count;
    
    for (uint32_t i = 0; i < attr_count; i++)
    {
        if (attr[i].id == SAI_FDB_ENTRY_ATTR_BRIDGE_PORT_ID)
            bport_id = attr[i].value.oid;
    }
           
    sai_fdb_entry_t fdb_m;
    sai_object_id_t b_id;
      
    switch (event_type)
    {   
        case SAI_FDB_EVENT_LEARNED:
            gFdbMap.emplace_back(std::pair<sai_fdb_entry_t, sai_object_id_t>(fdb_entry,bport_id));
            break;
        case SAI_FDB_EVENT_FLUSHED: 
            if (bv_id == 0 && bport_id == 0)
                gFdbMap.clear();
            else
            {
                for (auto it = gFdbMap.begin(); it != gFdbMap.end(); )
                {
                    fdb_m = it->first;
                    b_id = it->second; 				
	            
                    if (bport_id == 0 && bv_id == fdb_m.bv_id)
                        it = gFdbMap.erase(it);
                    else if (bv_id == 0 && bport_id == b_id)
                        it = gFdbMap.erase(it);
                    else if (bv_id == fdb_m.bv_id && bport_id == b_id)
                        it = gFdbMap.erase(it);
                    else
                        it++;
                }
            }
            break;
        case SAI_FDB_EVENT_MOVE:
            for (auto it = gFdbMap.begin(); it != gFdbMap.end(); it++)
            {
                fdb_m = it->first;
                b_id = it->second; 
                int n = memcmp ( fdb_entry.mac_address, fdb_m.mac_address, 6);
		    
                if (n == 0 && bv_id == fdb_m.bv_id)
                    it->second = bport_id;
            }
            break;  
        case SAI_FDB_EVENT_AGED:
            for (auto it = gFdbMap.begin(); it != gFdbMap.end(); )
            {
                fdb_m = it->first;
                b_id = it->second; 
                int n = memcmp ( fdb_entry.mac_address, fdb_m.mac_address, 6);  
                
                if (n == 0 && bv_id == fdb_m.bv_id)
                    it = gFdbMap.erase(it);   
                else
                    it++; 	
            }
            break;
        default:
            printf("unknown event");
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
    //UNREFERENCED_PARAMETER(profile_id);

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
    //UNREFERENCED_PARAMETER(profile_id);

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

#ifdef BRCMSAI
void sai_diag_shell()
{
    sai_status_t status;

    while (true)
    {
        sai_attribute_t attr;
        attr.id = SAI_SWITCH_ATTR_SWITCH_SHELL_ENABLE;
        attr.value.booldata = true;
        status = sai_switch_api->set_switch_attribute(gSwitchId, &attr);
        if (status != SAI_STATUS_SUCCESS)
        {
            return;
        }

        sleep(1);
    }
}
#endif

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

        size_t pos = line.find(" ");

        if (pos == std::string::npos)
        {
            printf("not found ' ' in line %s\n", line.c_str());
            continue;
        }

        std::string fp_value = line.substr(0, pos);
        std::string lanes    = line.substr(pos + 1);

        lanes.erase(std::remove_if(lanes.begin(), lanes.end(), ::isspace), lanes.end());

        std::istringstream iss(lanes);
        std::string lane_str;
        std::set<int> lane_set;

        while (getline(iss, lane_str, ','))
        {
            int lane = stoi(lane_str);
            lane_set.insert(lane);
        }

        gPortMap.insert(std::pair<std::set<int>,std::string>(lane_set,fp_value));
    }
}

void handleInitScript(const std::string& initScript)
{

    if (initScript.size() == 0)
        return;

    printf("Running %s ...\n", initScript.c_str());
    system(initScript.c_str());
}

/*void initialize_sai_api(sai_service_method_table_t * services) {
    // Open the shared library (if using dynamic loading)
    void *handle = dlopen("/SAI/SAI/meta/libsai.so", RTLD_LAZY);
    if (!handle) {
        printf("Error loading library: %s\n", dlerror());
        return;
    }

	global_apis->api_initialize = reinterpret_cast<sai_api_initialize_fn>(dlsym(handle, "sai_api_initialize"));
	if (global_apis->api_initialize == NULL) {
		printf("Failed to load sai_api_initialize: %s\n", dlerror());
		dlclose(handle);
		return;
	}

    sai_status_t status;
    uint64_t flags = 0; 

    status = global_apis->api_initialize(flags, services);

    if (status != SAI_STATUS_SUCCESS) {
        printf("SAI API initialization failed %d\n",status);
    } else {
        printf("SAI API initialized successfully\n");
    }

    // Close the shared library after use
    dlclose(handle);
}*/

int main(int argc, char **argv)
{
    int rv = 0;
    //api_implemented();
    // Initialize sai_global_apis_t
    sai_global_apis_t apis;
    global_apis = &apis;

    auto options = handleCmdLine(argc, argv);
    handleProfileMap(options.profileMapFile);
    handlePortMap(options.portMapFile);

	//initialize_sai_api((sai_service_method_table_t *)&test_services);
    //auto status = sai_api_initialize(0, (sai_service_method_table_t *)&test_services);
    //sai_api_query(SAI_API_SWITCH, (void**)&sai_switch_api);
    
    constexpr std::uint32_t attrSz = 5;

    sai_attribute_t attr[attrSz];

    std::memset(attr, '\0', sizeof(attr));

    attr[0].id = SAI_SWITCH_ATTR_INIT_SWITCH;
    attr[0].value.booldata = true;

    attr[1].id = SAI_SWITCH_ATTR_SWITCH_STATE_CHANGE_NOTIFY;
    attr[1].value.ptr = reinterpret_cast<sai_pointer_t>(&on_switch_state_change);

    attr[2].id = SAI_SWITCH_ATTR_SHUTDOWN_REQUEST_NOTIFY;
    attr[2].value.ptr = reinterpret_cast<sai_pointer_t>(&on_shutdown_request);

    attr[3].id = SAI_SWITCH_ATTR_FDB_EVENT_NOTIFY;
    attr[3].value.ptr = reinterpret_cast<sai_pointer_t>(&on_fdb_event);

    attr[4].id = SAI_SWITCH_ATTR_PORT_STATE_CHANGE_NOTIFY;
    attr[4].value.ptr = reinterpret_cast<sai_pointer_t>(&on_port_state_change);

    sai_attribute_t attr_pkt;
    attr_pkt.id = SAI_SWITCH_ATTR_PACKET_EVENT_NOTIFY;
    attr_pkt.value.ptr = reinterpret_cast<sai_pointer_t>(&on_packet_event);
    
    //handleInitScript(options.initScript);
    
    start_sai_thrift_rpc_server(SWITCH_SAI_THRIFT_RPC_SERVER_PORT);

    while (true)
    {
        pause();
    }

    return 0;
}
